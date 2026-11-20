# Architecture

## System context

Clients submit orders in three formats to one intake service. Everything after that runs through RabbitMQ. The providers (payment, inventory, shipping, notification) and the pricing service are external HTTP systems, here the kit's mocks. An operator watches the system through the observer and acts through endpoints on intake and the orchestrator.

```mermaid
flowchart LR
  client["Clients: web JSON, mobile JSON, b2b XML"]
  operator["Operator"]
  pricing["pricing :4110"]
  providers["payment, inventory, shipping, notification mocks :4801-4804"]
  subgraph capstone["Order system, Docker Compose project capstone"]
    intake["cap-intake :8800"]
    orch["cap-orchestrator :8802"]
    notifier["cap-notifier :8803"]
    observer["cap-observer :8801"]
    broker[("cap-rabbitmq")]
  end
  client -->|"POST /orders"| intake
  intake -->|"enrich each item"| pricing
  intake -->|"order.accepted"| broker
  broker -->|"saga.queue"| orch
  orch -->|"saga steps over HTTP"| providers
  orch -->|"lifecycle and outcome events"| broker
  broker -->|"failed, compensated"| notifier
  notifier -->|"order_not_completed notice"| providers
  intake -->|"hop records"| broker
  orch -->|"hop records"| broker
  notifier -->|"hop records"| broker
  broker -->|"trace.store"| observer
  operator -->|"GET /trace, GET /ops/metrics"| observer
  operator -->|"GET /dlq, POST /dlq/replay"| intake
  operator -->|"GET /sagas/stuck, POST /sagas/id/compensate"| orch
```

## Integration architecture

Every exchange and queue of the topology is shown. The solid paths are the business flow. The retry paths use a TTL queue and return to the origin queue only. After MAX_RETRIES attempts a message goes to the dead letter channel. A message that cannot be parsed goes to the invalid message channel and is never retried. Every service publishes hop records to the wire tap, and the observer stores them.

```mermaid
flowchart TB
  intake["intake"]
  orch["orchestrator"]
  notifier["notifier"]
  observer["observer"]
  OX["orders.exchange, fanout"]
  SQ["saga.queue"]
  RETX["orders.retry.exchange, direct"]
  SRQ["saga.retry.queue, TTL 1 s"]
  RET["orders.return.exchange, direct"]
  OUT["orders.outcome, topic"]
  NQ["notifications.queue"]
  NRQ["notifications.retry.queue, TTL 1 s"]
  DLX["orders.dlq.exchange, fanout"]
  DLQ["orders.dlq"]
  INVX["orders.invalid.exchange, fanout"]
  INVQ["orders.invalid"]
  TE["trace.events, fanout, wire tap"]
  TS["trace.store"]
  IOQ["intake.outcome.queue"]

  intake -->|"order.accepted, persistent, confirmed"| OX
  OX --> SQ
  SQ --> orch
  SQ -->|"nack, dead-letter"| RETX
  RETX -->|"key saga.queue"| SRQ
  SRQ -->|"after TTL"| RET
  RET -->|"key saga.queue"| SQ
  orch -->|"order.processing ... order.completed"| OUT
  OUT -->|"order.failed, order.compensated"| NQ
  OUT -->|"all events"| IOQ
  IOQ -->|"order state view"| intake
  NQ --> notifier
  NQ -->|"nack, dead-letter"| RETX
  RETX -->|"key notifications.queue"| NRQ
  NRQ -->|"after TTL"| RET
  RET -->|"key notifications.queue"| NQ
  orch -->|"after MAX_RETRIES"| DLX
  notifier -->|"after MAX_RETRIES"| DLX
  DLX --> DLQ
  intake -->|"POST /dlq/replay"| RET
  orch -->|"malformed message"| INVX
  notifier -->|"malformed message"| INVX
  INVX --> INVQ
  intake -.->|"hop records"| TE
  orch -.->|"hop records"| TE
  notifier -.->|"hop records"| TE
  TE --> TS
  TS --> observer
```

## Known limits

- One instance of each service. Competing consumers are not used.
- Outcome events are at-least-once. Intake and the notifier tolerate duplicates.
- Hop records are fire-and-forget on a channel without confirms, so a hop can be lost. A lost hop never fails an order.
- The saga.stuck and saga.operator_compensate hops have no causationId, because the orchestrator does not store the last hop of a crashed saga.
