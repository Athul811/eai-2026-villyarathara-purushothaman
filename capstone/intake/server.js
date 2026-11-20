import express from "express";
import amqp from "amqplib";

const PORT = Number(process.env.PORT || 8800);
let conn = null;

async function connect() {
  try {
    conn = await amqp.connect(process.env.RABBITMQ_URL);
    conn.on("close", () => { conn = null; setTimeout(connect, 1000); });
    conn.on("error", () => {});
  } catch {
    conn = null;
    setTimeout(connect, 1000);
  }
}
connect();

const app = express();
app.get("/health", (_req, res) => {
  const up = conn !== null;
  res.status(up ? 200 : 503).json({
    status: up ? "ok" : "degraded",
    checks: { broker: up ? "up" : "down", consumers: {} },
  });
});
app.listen(PORT, () => console.log(`intake listening on ${PORT}`));
