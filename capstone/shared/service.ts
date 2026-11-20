import express from "express";
import type { Express } from "express";
import amqp from "amqplib";
import type { Channel, ConsumeMessage } from "amqplib";

type Setup = (ch: Channel) => Promise<void>;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Connects, runs setups (consumers), and reconnects at runtime (R-4.9.14/15). */
export class Broker {
  ch?: Channel;
  up = false;
  consumers: Record<string, number> = {};
  private setups: Setup[] = [];
  constructor(private url: string) {}

  consume(
    queue: string,
    handler: (msg: ConsumeMessage, ch: Channel) => Promise<void> | void,
    prefetch = 1,
  ): void {
    this.consumers[queue] = 0;
    this.setups.push(async (ch) => {
      await ch.prefetch(prefetch);
      await ch.consume(
        queue,
        (msg) => {
          if (msg) Promise.resolve(handler(msg, ch)).catch((e) => console.error(`[${queue}]`, e));
        },
        { noAck: false },
      );
      this.consumers[queue] = 1;
    });
  }

  async run(): Promise<void> {
    for (;;) {
      try {
        await this.connectOnce();
        return;
      } catch (e) {
        console.warn("[broker] connect failed:", (e as Error).message, "- retrying in 1s");
        await sleep(1000);
      }
    }
  }

  private async connectOnce(): Promise<void> {
    const conn = await amqp.connect(this.url);
    try {
      const ch = await conn.createChannel();
      ch.on("error", (e: Error) => console.error("[broker] channel error:", e.message));
      ch.on("close", () => void conn.close().catch(() => {}));
      for (const s of this.setups) await s(ch);
      this.ch = ch;
      this.up = true;
    } catch (e) {
      await conn.close().catch(() => {});
      throw e;
    }
    conn.on("error", (e: Error) => console.error("[broker] connection error:", e.message));
    conn.on("close", () => {
      console.warn("[broker] connection closed, reconnecting");
      this.up = false;
      this.ch = undefined;
      for (const q of Object.keys(this.consumers)) this.consumers[q] = 0;
      void this.run();
    });
  }
}

export function createService(): { app: Express; broker: Broker; start: () => void } {
  const app = express();
  const broker = new Broker(process.env.RABBITMQ_URL!);

  app.get("/health", (_req, res) => {
    const ok = broker.up && Object.values(broker.consumers).every((n) => n > 0);
    res.status(ok ? 200 : 503).json({
      status: ok ? "ok" : "degraded",
      checks: { broker: broker.up ? "up" : "down", consumers: { ...broker.consumers } },
    });
  });

  const start = () => {
    const port = Number(process.env.PORT);
    app.listen(port, () => console.log(`listening on :${port}`));
    void broker.run();
  };
  return { app, broker, start };
}
