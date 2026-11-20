# Capstone: order processing system

## How to run

From this folder: `docker compose up -d --build --wait`. Docker Compose 2.20 or later and about 4 GB of memory for Docker are needed. The public tests run with `node tests/stage.mjs 6`. Reset everything with `docker compose down -v`.

## Services

intake (8800), observer (8801), orchestrator (8802) and notifier (8803), with RabbitMQ and the provider mocks from the kit. The code is JavaScript (Node 20).

## Known limitations

- Shipping calls carry no Idempotency-Key, so a repeated create call can make a second shipment.
- After a crash mid-shipping, the shipment can exist at the provider while the saga is needs_attention, and operator compensation refunds the payment and releases the inventory but does not cancel the shipment (see docs/adr/adr-010.md).
- State is kept in JSON files rewritten in full on each change.

## Advanced component

Not implemented.
