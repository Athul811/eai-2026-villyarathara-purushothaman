import { randomUUID } from "node:crypto";
import express from "express";
import type { NextFunction, Request, Response } from "express";
import type { GetMessage } from "amqplib";
import { connectWithRetry, withoutRetryHistory } from "../../shared/rabbit";

const ORDERS_EXCHANGE = "orders.exchange";
const RETURN_EXCHANGE = "orders.return.exchange";
const DLQ_EXCHANGE = "orders.dlq.exchange";
const DLQ_QUEUE = "orders.dlq";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const PORT = Number(process.env.PORT ?? "3000");

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  // Replay reads from the DLQ, so order-service declares it too.
  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue(DLQ_QUEUE, { durable: true });
  await channel.bindQueue(DLQ_QUEUE, DLQ_EXCHANGE, "");
  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  const orders = new Map<string, unknown>();
  const app = express();
  app.use(express.json());

  app.get("/health", (_req: Request, res: Response) => {
    res.json({ status: "ok" });
  });

  app.post("/orders", (req: Request, res: Response) => {
    const order = req.body as { orderId?: unknown; items?: unknown } | undefined;
    if (
      !order ||
      typeof order !== "object" ||
      typeof order.orderId !== "string" ||
      order.orderId === "" ||
      !Array.isArray(order.items)
    ) {
      res.status(400).json({ error: "body must be a canonical order with orderId and items" });
      return;
    }

    // correlationId travels in a header: the canonical schema forbids extra
    // body fields (additionalProperties: false).
    const correlationId = randomUUID();
    orders.set(correlationId, order);
    channel.publish(ORDERS_EXCHANGE, "", Buffer.from(JSON.stringify(order)), {
      headers: { correlationId },
      contentType: "application/json",
      persistent: true,
    });
    res.status(201).json({ correlationId, status: "accepted" });
  });

  app.get("/orders/:correlationId", (req: Request, res: Response) => {
    const id = String(req.params.correlationId);
    const order = orders.get(id);
    if (order === undefined) {
      res.status(404).json({ error: "not found" });
      return;
    }
    res.json({ correlationId: id, order });
  });

  app.post("/dlq/replay", async (_req: Request, res: Response) => {
    let replayed = 0;
    const skipped: GetMessage[] = [];
    try {
      for (;;) {
        const msg = await channel.get(DLQ_QUEUE, { noAck: false });
        if (!msg) break;

        const headers = msg.properties.headers ?? {};
        const origin = headers["originQueue"] as string | undefined;
        if (!origin) {
          // Unknown origin: hold it unacked until the loop ends, then put it
          // back, so we never spin on the same message.
          skipped.push(msg);
          continue;
        }

        // Back through the DIRECT return exchange, routed to the failing
        // consumer's queue only, with a fresh retry history.
        channel.publish(RETURN_EXCHANGE, origin, msg.content, {
          headers: withoutRetryHistory(headers),
          contentType: msg.properties.contentType,
          persistent: true,
        });
        // Ack only after republishing.
        channel.ack(msg);
        replayed += 1;
      }
    } finally {
      for (const msg of skipped) {
        channel.nack(msg, false, true);
      }
    }
    res.json({ replayed });
  });

  // Malformed JSON bodies get a 400 instead of a crash.
  app.use((err: Error, _req: Request, res: Response, _next: NextFunction) => {
    res.status(400).json({ error: err.message });
  });

  app.listen(PORT, () => {
    console.log(`[Order] listening on :${PORT}`);
  });
}

main().catch(console.error);
