# Runbook

All commands run from the capstone folder on the host. The observer's /ops/metrics shows the alerts and the queue figures. ready and consumers come from a passive queue declare and are exact. unacked comes from RabbitMQ's management API and can be a few seconds late.

## dlq_not_empty

**Meaning.** At least one message was dead-lettered: a saga whose first step kept failing (state parked), or a failure notice the notifier could not deliver after MAX_RETRIES attempts.

**Likely causes.** (1) The payment provider (inventory for b2b) was down or timing out for 3 attempts. (2) The notification provider failed order_not_completed notices.

**Diagnose.**

    curl -s http://localhost:8800/dlq
    curl -s http://localhost:8800/orders/<correlationId>
    curl -s http://localhost:8801/trace/<correlationId>

originQueue says which side failed (saga.queue or notifications.queue) and lastError says why.

**Fix.** Repair the cause first (the provider), then replay one entry or all:

    curl -s -X POST http://localhost:8800/dlq/replay -H "Content-Type: application/json" -d '{"correlationId":"<correlationId>"}'
    curl -s -X POST http://localhost:8800/dlq/replay -H "Content-Type: application/json" -d '{}'

**Verify.** GET /dlq shows count 0. The order moves from parked to completed (or the notice is in /data/notification.log). dlq_not_empty clears in /ops/metrics, allowing a few seconds for unacked to settle. A replay with the cause still present returns the message to the DLQ after fresh attempts.

## invalid_not_empty

**Meaning.** A message that could not be parsed or validated was quarantined in orders.invalid with its original bytes and headers. It is never retried and never replayed.

**Likely causes.** (1) A producer published something that is not JSON or not a valid canonical order, or without a correlationId header. (2) A manual publish from the management UI.

**Diagnose.** Open http://localhost:15680 (user eai), Queues, orders.invalid, Get messages, and read the headers and payload. Then check which service quarantined it:

    docker compose logs orchestrator notifier --tail 50

**Fix.** Fix the producer. There is no endpoint to purge orders.invalid, so after analysis purge it in the management UI.

**Verify.** invalidDepth is 0 in /ops/metrics and the alert clears. A new valid order still completes.

## saga_consumer_missing

**Meaning.** saga.queue has no consumer, so no order is being processed. Messages wait safely in the durable queue.

**Likely causes.** (1) The orchestrator container is stopped or crashed. (2) It lost the broker connection and is still reconnecting, for example after a broker restart.

**Diagnose.**

    docker compose ps
    docker compose logs orchestrator --tail 50
    curl -s http://localhost:8802/health

**Fix.** If the container is down, run docker compose up -d orchestrator. After a broker restart, wait up to 30 s for the reconnect. If it does not come back, restart the orchestrator. On restart, sagas that were mid-flight are marked needs_attention (see stuck_sagas).

**Verify.** /ops/metrics shows 1 consumer on saga.queue, ready drains to 0, and the alert clears.

## notifier_consumer_missing

**Meaning.** notifications.queue has no consumer, so no failure notices are sent. The events wait in the durable queue.

**Likely causes.** (1) The notifier container is stopped or crashed. (2) It lost the broker connection and is reconnecting.

**Diagnose.**

    docker compose ps
    docker compose logs notifier --tail 50
    curl -s http://localhost:8803/health

**Fix.** docker compose up -d notifier, or restart it. The notifier checks its processed ids before it sends, so a redelivered event does not send a second notice.

**Verify.** /ops/metrics shows 1 consumer on notifications.queue, ready drains, and the alert clears.

## saga_consumer_lag

**Meaning.** More than 5 messages are ready on saga.queue. The orchestrator is a full batch behind.

**Threshold.** 5, equal to SAGA_CONCURRENCY and the prefetch. Up to 5 orders are in flight (unacked, not ready), so more than 5 ready means every slot is busy and a further batch is waiting. This figure was chosen from that reasoning, not measured under load.

**Likely causes.** (1) A slow provider, for example shipping, keeps all 5 slots busy. (2) The orchestrator is down (saga_consumer_missing fires too). (3) A burst of submissions.

**Diagnose.**

    curl -s http://localhost:8801/ops/metrics
    curl -s http://localhost:4803/admin/config
    docker compose logs orchestrator --tail 50

**Fix.** Fix or wait out the slow provider. If the load is real, raise SAGA_CONCURRENCY in docker-compose.yml and recreate the orchestrator.

**Verify.** ready on saga.queue falls to 5 or below and the alert clears.

## notifier_consumer_lag

**Meaning.** More than 5 messages are ready on notifications.queue. Failure notices are not being sent as fast as outcomes arrive.

**Threshold.** 5, for the same reason. A notice is one fast HTTP call, so more than 5 waiting means the notifier is stopped or the provider is slow. Chosen, not measured.

**Likely causes.** (1) The notifier is stopped (notifier_consumer_missing fires too). (2) The notification provider is slow, so each notice waits up to REQUEST_TIMEOUT_MS.

**Diagnose.**

    curl -s http://localhost:8801/ops/metrics
    docker compose ps notifier
    curl -s http://localhost:4804/admin/config

**Fix.** Start the notifier, or repair the notification provider. Messages that exhaust their retries go to the DLQ, see dlq_not_empty.

**Verify.** ready on notifications.queue drains to 0 and the alert clears.

## stuck_sagas

**Meaning.** At least one saga is in needs_attention (the orchestrator restarted while its steps were running) or compensation_failed (a compensation call failed). Nothing runs again automatically.

**Likely causes.** (1) The orchestrator crashed or was restarted mid-saga. (2) A refund or release call failed, for example the provider was down.

**Diagnose.**

    curl -s http://localhost:8802/sagas/stuck
    curl -s "http://localhost:4801/admin/effects?orderId=<orderId>"
    curl -s http://localhost:8801/trace/<correlationId>

Check the provider effects first, because the shipment may already have been created and cannot be undone.

**Fix.** After confirming the effects, compensate. This refunds payment and releases inventory, so a person must decide it:

    curl -s -X POST http://localhost:8802/sagas/<correlationId>/compensate

A repeat call for a saga already compensated this way returns the same answer and does nothing. A false positive is possible: a saga that actually finished at the providers but was not recorded would be refunded.

**Verify.** The answer is compensated, /sagas/stuck no longer lists it, and /admin/effects shows one refund and one release. stuck_sagas clears.
