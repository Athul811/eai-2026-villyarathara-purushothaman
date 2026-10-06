"""
Aggregator Service
==================
Your job: implement the AGGREGATOR pattern on top of the connection
handling and consume loop already wired up below.

- Collect item results from orders.results, grouped by orderId.
- Completion condition: every item of the order has reported a result.
- Timeout: if the order has been sitting incomplete for too long (one
  worker crashed, or was never running), emit a PARTIAL result instead of
  waiting forever. A hung order is a worse outcome than an honest partial
  answer.
- Duplicate results (the same item redelivered, e.g. after a requeue) must
  not be double-counted.
- Two orders in flight at once must never have their results mixed up.

Consumes from: orders.results
Publishes to:  orders.complete

Required shape of the message you publish to orders.complete (tests depend
on every one of these fields):

    {
        "orderId": "<the order this result set belongs to>",
        "correlationId": "<same value as orderId>",
        "status": "complete" | "partial",
        "totalItems": <the totalItems every item message carried>,
        "receivedItems": <how many distinct items you actually collected>,
        "itemResults": [ <the result messages you received, in any order> ],
        "missingItemIndexes": [ <itemIndex values you never received>
                                 -- empty list when status == "complete" ]
    }

Publish exactly ONE message to orders.complete per order, whichever status
it ends up with.
"""

import json
import pika
import os
import threading
import time


# in_flight: orderId -> {
#     'total_items': int,
#     'results': {itemIndex: result_dict, ...},   # keyed by itemIndex so a
#                                                   # redelivered duplicate
#                                                   # overwrites instead of
#                                                   # double-counting
#     'last_activity': float,                      # time.time() of the
#                                                   # last result recorded
# }
in_flight = {}
lock = threading.Lock()

# How long an order can sit with no NEW results before we give up and emit
# a partial. Measured from last activity, not order arrival, so a large
# order whose workers are all still actively answering never times out
# mid-flight. See docs/adr-002.md for the chosen value and reasoning.
IDLE_TIMEOUT_SECONDS = float(os.environ.get('AGGREGATOR_IDLE_TIMEOUT_SECONDS', '5'))

# How often the background sweep checks for timed-out orders. Independent
# of IDLE_TIMEOUT_SECONDS; this just controls how promptly a timeout is
# noticed once it has actually elapsed.
SWEEP_INTERVAL_SECONDS = 1.0


def get_rabbitmq_connection():
    """Create a connection to RabbitMQ using environment variable for host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(host=os.environ.get('RABBITMQ_HOST', 'localhost'))
    )


def publish_completion(message):
    """Publish a single message to orders.complete. Called with the lock
    already released -- do not hold `lock` while doing network I/O."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()
    channel.queue_declare(queue='orders.complete', durable=True)
    channel.basic_publish(
        exchange='',
        routing_key='orders.complete',
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)  # Persistent
    )
    connection.close()


def _build_message(order_id, correlation_id, total_items, results, status):
    """Build a completion message (complete or partial) from an order's
    current state. Pure function, no I/O, no lock -- safe to call from
    either aggregate_result or sweep_timeouts."""
    received_indexes = set(results.keys())
    all_indexes = set(range(total_items))
    missing = sorted(all_indexes - received_indexes)

    return {
        "orderId": order_id,
        "correlationId": correlation_id,
        "status": status,
        "totalItems": total_items,
        "receivedItems": len(results),
        "itemResults": list(results.values()),
        "missingItemIndexes": missing,
    }


def aggregate_result(ch, method, properties, body):
    """
    Handle one message from orders.results:
    1. Parse it.
    2. Record it against the right order, keyed by itemIndex so a
       redelivered duplicate is a no-op rather than a second entry.
    3. Update that order's last-activity timestamp (for the sweep).
    4. If every expected item has now been recorded, build the "complete"
       message and publish it, then drop the order from in-flight state.
    5. Ack regardless -- an unparseable message must not jam the queue.
    """
    try:
        result = json.loads(body)
        order_id = result['orderId']
        correlation_id = result.get('correlationId', order_id)
        item_index = result['itemIndex']
        total_items = result['totalItems']
    except (json.JSONDecodeError, KeyError) as exc:
        print(f"[Aggregator] WARNING: dropping unparseable result: {exc}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    to_publish = None

    with lock:
        order = in_flight.setdefault(order_id, {
            'total_items': total_items,
            'results': {},
            'last_activity': time.time(),
        })

        order['results'][item_index] = result
        order['last_activity'] = time.time()

        if len(order['results']) >= order['total_items']:
            to_publish = _build_message(
                order_id, correlation_id, order['total_items'],
                order['results'], status="complete",
            )
            del in_flight[order_id]

    if to_publish is not None:
        publish_completion(to_publish)
        print(f"[Aggregator] Order {order_id} complete "
              f"({to_publish['receivedItems']}/{to_publish['totalItems']} items)")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    """
    Runs forever in a background thread, started from main(). Every
    SWEEP_INTERVAL_SECONDS, look for orders that have gone quiet for
    longer than IDLE_TIMEOUT_SECONDS and emit a partial result for each.
    """
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)

        to_publish_list = []
        now = time.time()

        with lock:
            timed_out_ids = [
                order_id
                for order_id, order in in_flight.items()
                if now - order['last_activity'] > IDLE_TIMEOUT_SECONDS
            ]

            for order_id in timed_out_ids:
                order = in_flight.pop(order_id)
                message = _build_message(
                    order_id, order_id, order['total_items'],
                    order['results'], status="partial",
                )
                to_publish_list.append(message)

        for message in to_publish_list:
            publish_completion(message)
            print(f"[Aggregator] Order {message['orderId']} timed out -- "
                  f"partial result ({message['receivedItems']}/"
                  f"{message['totalItems']} items, missing "
                  f"{message['missingItemIndexes']})")


def main():
    """Main entry point: connect to RabbitMQ, start the timeout sweeper,
    and start consuming results."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue='orders.results', durable=True)
    channel.queue_declare(queue='orders.complete', durable=True)

    channel.basic_qos(prefetch_count=1)

    sweeper = threading.Thread(target=sweep_timeouts, daemon=True)
    sweeper.start()

    channel.basic_consume(queue='orders.results', on_message_callback=aggregate_result)

    print('[Aggregator] Waiting for results...')
    channel.start_consuming()


if __name__ == '__main__':
    main()
