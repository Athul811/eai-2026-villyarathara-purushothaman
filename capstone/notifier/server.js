import { makeHop, emitHop, attachTrace } from "./hops.js";
import express from "express";
import amqp from "amqplib";
import { randomUUID } from "node:crypto";
import { readFileSync, writeFileSync, renameSync, appendFileSync, mkdirSync } from "node:fs";

const PORT = Number(process.env.PORT || 8803);
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const NOTIFICATION_URL = process.env.NOTIFICATION_URL;
const TIMEOUT = Number(process.env.REQUEST_TIMEOUT_MS || 2500);
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 3);
const DATA_DIR = process.env.DATA_DIR || "/data";
const now = () => new Date().toISOString();

mkdirSync(DATA_DIR, { recursive: true });
let processed = new Set();
try { processed = new Set(JSON.parse(readFileSync(`${DATA_DIR}/processed-ids.json`, "utf8"))); } catch {}
const saveProcessed = () => {
  const p = `${DATA_DIR}/processed-ids.json`;
  writeFileSync(`${p}.tmp`, JSON.stringify([...processed])); renameSync(`${p}.tmp`, p);
};

let conn = null, ch = null, consuming = false;
function confirmPub(exchange, key, content, options) {
  return new Promise((resolve, reject) => {
    if (!ch) return reject(new Error("no broker"));
    const t = setTimeout(() => reject(new Error("confirm timeout")), 5000);
    try { ch.publish(exchange, key, content, options, (e) => { clearTimeout(t); e ? reject(e) : resolve(); }); }
    catch (e) { clearTimeout(t); reject(e); }
  });
}
const ack = (m) => { try { ch.ack(m); } catch {} };
const nack = (m) => { try { ch.nack(m, false, false); } catch {} };

async function connect() {
  try {
    conn = await amqp.connect(RABBITMQ_URL);
    conn.on("error", () => {});
    conn.on("close", () => { conn = null; ch = null; consuming = false; setTimeout(connect, 1000); });
    ch = await conn.createConfirmChannel();
    await attachTrace(conn);
    ch.on("error", () => {});
    ch.on("close", () => { ch = null; consuming = false; conn?.close().catch(() => {}); });
    await ch.prefetch(10);
    await ch.consume("notifications.queue", (msg) => {
      if (!msg) return;
      handle(msg).catch((e) => { console.error(e); nack(msg); });
    });
    consuming = true;
  } catch (e) {
    console.error("connect failed", e?.message);
    ch = null; consuming = false;
    try { await conn?.close(); } catch {}
    conn = null;
    setTimeout(connect, 1000);
  }
}

const retryCount = (h) => {
  const d = (h || {})["x-death"];
  if (!Array.isArray(d)) return 0;
  return d.filter((e) => e.queue === "notifications.queue" && e.reason === "rejected")
    .reduce((n, e) => n + Number(e.count || 0), 0);
};

async function quarantine(msg) {
  await confirmPub("orders.invalid.exchange", "", msg.content,
    { persistent: true, messageId: randomUUID(), headers: msg.properties.headers || {} });
  ack(msg);
}

async function send(ev, cid, causationId) {
  try {
    const r = await fetch(`${NOTIFICATION_URL}/notification/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Correlation-Id": cid, "X-Causation-Id": causationId },
      body: JSON.stringify({ orderId: ev.orderId, recipient: ev.recipient, kind: "order_not_completed", reason: ev.code }),
      signal: AbortSignal.timeout(TIMEOUT),
    });
    return r.ok ? { ok: true } : { ok: false, error: `notification ${r.status}` };
  } catch (e) {
    return { ok: false, error: e?.name === "TimeoutError" || e?.name === "AbortError" ? "timeout" : "unreachable" };
  }
}

async function handle(msg) {
  const h = msg.properties.headers || {};
  const cid = h.correlationId ? String(h.correlationId) : null;
  const attempt = retryCount(h) + 1;
  let ev;
  try {
    ev = JSON.parse(msg.content.toString("utf8"));
    if (!cid || !ev || typeof ev !== "object" || !ev.orderId || !ev.recipient) throw new Error("invalid");
  } catch {
    if (cid) emitHop(makeHop({ service: "notifier", operation: "invalid.quarantine", status: "quarantined", correlationId: cid,
      causationId: h.causationId ?? null, messageId: msg.properties.messageId, startedAt: new Date() }));
    return quarantine(msg);
  }

  let last = h.causationId ?? null;
  const hop = (operation, status, t0, o = {}) => {
    const x = makeHop({ service: "notifier", operation, status, correlationId: cid,
      causationId: o.causedBy !== undefined ? o.causedBy : last, attempt, messageId: msg.properties.messageId,
      startedAt: t0, hopId: o.hopId, detail: o.detail });
    emitHop(x); last = x.hopId; return x.hopId;
  };
  const consume = hop("notify.consume", "success", new Date());

  if (processed.has(cid)) { hop("notify.duplicate", "duplicate", new Date()); return ack(msg); }

  const sid = randomUUID(); const t0 = new Date();
  const r = await send(ev, cid, sid);
  hop("notify.send", r.ok ? "success" : (r.error === "timeout" ? "timeout" : "failed"), t0,
    { hopId: sid, detail: r.ok ? undefined : { code: r.error } });
  if (r.ok) {
    appendFileSync(`${DATA_DIR}/notification.log`, JSON.stringify({ at: now(), correlationId: cid,
      orderId: ev.orderId, recipient: ev.recipient, kind: "order_not_completed", reason: ev.code }) + "\n");
    processed.add(cid); saveProcessed();
    return ack(msg);
  }
  const attempts = attempt;
  if (attempts < MAX_RETRIES) { hop("notify.retry", "retried", new Date(), { causedBy: consume }); return nack(msg); }
  const dl = hop("notify.dead_letter", "dead_lettered", new Date(), { causedBy: consume, detail: { code: r.error } });
  await confirmPub("orders.dlq.exchange", "", msg.content, {
    persistent: true, messageId: randomUUID(), contentType: "application/json",
    headers: { correlationId: cid, causationId: dl, originQueue: "notifications.queue",
      lastError: r.error, attempts, deadLetteredAt: now() },
  });
  ack(msg);
}

const app = express();
app.get("/health", (_req, res) => {
  const up = conn !== null && ch !== null && consuming;
  res.status(up ? 200 : 503).json({ status: up ? "ok" : "degraded",
    checks: { broker: up ? "up" : "down", consumers: { "notifications.queue": consuming ? 1 : 0 } } });
});
app.listen(PORT, () => console.log(`notifier listening on ${PORT}`));
connect();
