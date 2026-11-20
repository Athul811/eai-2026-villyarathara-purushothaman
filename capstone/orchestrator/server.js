import express from "express";
import amqp from "amqplib";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { randomUUID } from "node:crypto";
import { readFileSync, writeFileSync, renameSync, mkdirSync } from "node:fs";

const PORT = Number(process.env.PORT || 8802);
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const TIMEOUT = Number(process.env.REQUEST_TIMEOUT_MS || 2500);
const MAX_RETRIES = Number(process.env.MAX_RETRIES || 3);
const CONC = Number(process.env.SAGA_CONCURRENCY || 5);
const DATA_DIR = process.env.DATA_DIR || "/data";
const URLS = { payment: process.env.PAYMENT_URL, inventory: process.env.INVENTORY_URL,
  shipping: process.env.SHIPPING_URL, notification: process.env.NOTIFICATION_URL };
const RUN_ID = randomUUID();
const now = () => new Date().toISOString();

mkdirSync(DATA_DIR, { recursive: true });
let store = { sagas: {}, businessKeys: {} };
try { store = JSON.parse(readFileSync(`${DATA_DIR}/saga-store.json`, "utf8")); } catch {}
const persist = () => { const p = `${DATA_DIR}/saga-store.json`; writeFileSync(`${p}.tmp`, JSON.stringify(store)); renameSync(`${p}.tmp`, p); };

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validate = ajv.compile(JSON.parse(readFileSync("/canonical/order.schema.json", "utf8")));

const OUTCOME_STATES = new Set(["completed", "failed", "compensated", "compensation_failed", "needs_attention", "duplicate"]);
const running = new Set();
const compensating = new Set();

let conn = null, ch = null, consuming = false, scanned = false;
function confirmPub(exchange, key, content, options) {
  return new Promise((resolve, reject) => {
    if (!ch) return reject(new Error("no broker"));
    const t = setTimeout(() => reject(new Error("confirm timeout")), 5000);
    try { ch.publish(exchange, key, content, options, (e) => { clearTimeout(t); e ? reject(e) : resolve(); }); }
    catch (e) { clearTimeout(t); reject(e); }
  });
}
const pubOutcome = (rk, env) => confirmPub("orders.outcome", rk, Buffer.from(JSON.stringify(env)),
  { persistent: true, contentType: "application/json", messageId: randomUUID(),
    headers: { correlationId: env.correlationId, causationId: randomUUID() } });
const ack = (m) => { try { ch.ack(m); } catch {} };
const nack = (m) => { try { ch.nack(m, false, false); } catch {} };

async function connect() {
  try {
    conn = await amqp.connect(RABBITMQ_URL);
    conn.on("error", () => {});
    conn.on("close", () => { conn = null; ch = null; consuming = false; setTimeout(connect, 1000); });
    ch = await conn.createConfirmChannel();
    ch.on("error", () => {});
    ch.on("close", () => { ch = null; consuming = false; conn?.close().catch(() => {}); });
    if (!scanned) { scanned = true; await scanStuck(); }
    await ch.prefetch(CONC);
    await ch.consume("saga.queue", (msg) => {
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

const envelope = (ctx, cid, state, extra = {}) => ({
  correlationId: cid, businessKey: ctx.businessKey, orderId: ctx.orderId, source: ctx.source,
  orderType: ctx.orderType, recipient: ctx.recipient, state, ...extra, occurredAt: now(),
});
const ctxOf = (order) => ({ businessKey: `${order.source}:${order.orderId}`, orderId: order.orderId,
  source: order.source, orderType: order.orderType, recipient: order.customer.email });

function cents(s) { const [a, b = "00"] = String(s).split("."); return Number(a) * 100 + Number(b.padEnd(2, "0").slice(0, 2)); }
function amount(order) {
  const c = order.items.reduce((sum, i) => sum + cents(i.unitPrice) * i.quantity, 0);
  return `${Math.floor(c / 100)}.${String(c % 100).padStart(2, "0")}`;
}

const STEP = {
  payment: (o, bk) => ({ url: `${URLS.payment}/payment/authorize`, key: `authorize:${bk}`, body: { orderId: o.orderId, amount: amount(o) } }),
  inventory: (o, bk) => ({ url: `${URLS.inventory}/inventory/reserve`, key: `reserve:${bk}`, body: { orderId: o.orderId, items: o.items } }),
  shipping: (o) => ({ url: `${URLS.shipping}/shipping/create`, body: { orderId: o.orderId } }),
  notification: (o) => ({ url: `${URLS.notification}/notification/send`, body: { orderId: o.orderId, recipient: o.customer.email } }),
};
const COMP = {
  inventory: { name: "inventory_release", spec: (id, bk) => ({ url: `${URLS.inventory}/inventory/release`, key: `release:${bk}`, body: { orderId: id } }) },
  payment: { name: "payment_refund", spec: (id, bk) => ({ url: `${URLS.payment}/payment/refund`, key: `refund:${bk}`, body: { orderId: id } }) },
};

async function call(spec, cid) {
  try {
    const r = await fetch(spec.url, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Correlation-Id": cid, "X-Causation-Id": randomUUID(),
        ...(spec.key ? { "Idempotency-Key": spec.key } : {}) },
      body: JSON.stringify(spec.body), signal: AbortSignal.timeout(TIMEOUT),
    });
    if (r.ok) return { ok: true };
    let code; try { code = (await r.json()).code; } catch {}
    return { ok: false, temp: r.status >= 500, code: code ?? `http_${r.status}` };
  } catch (e) {
    return { ok: false, temp: true, code: e?.name === "TimeoutError" || e?.name === "AbortError" ? "timeout" : "unreachable" };
  }
}

async function runComps(cid, saga, ctx) {
  for (const s of ["inventory", "payment"]) {
    if (!saga.steps.some((x) => x.step === s && x.status === "ok")) continue;
    const c = COMP[s];
    if (saga.compensations.some((x) => x.step === c.name && x.status === "ok")) continue;
    const r = await call(c.spec(ctx.orderId, ctx.businessKey), cid);
    saga.compensations.push({ step: c.name, status: r.ok ? "ok" : "failed", at: now() });
    persist();
    if (!r.ok) return { ok: false, code: r.code };
  }
  return { ok: true };
}

async function setOutcome(cid, saga, rk, env) {
  saga.state = env.state; saga.outcome = { routingKey: rk, envelope: env }; saga.updatedAt = now();
  persist();
  await pubOutcome(rk, env);
}

const retryCount = (h) => {
  const d = (h || {})["x-death"];
  if (!Array.isArray(d)) return 0;
  return d.filter((e) => e.queue === "saga.queue" && e.reason === "rejected").reduce((n, e) => n + Number(e.count || 0), 0);
};

async function scanStuck() {
  for (const [cid, s] of Object.entries(store.sagas)) {
    if (s.state === "processing" && s.owner !== RUN_ID && s.steps.some((x) => x.status === "ok")) {
      const env = { ...s.outcome.envelope, state: "needs_attention", occurredAt: now() };
      delete env.code;
      await setOutcome(cid, s, "order.needs_attention", env);
    }
  }
}

async function quarantine(msg) {
  await confirmPub("orders.invalid.exchange", "", msg.content,
    { persistent: true, messageId: randomUUID(), headers: msg.properties.headers || {} });
  ack(msg);
}

async function handle(msg) {
  const h = msg.properties.headers || {};
  const cid = h.correlationId ? String(h.correlationId) : null;
  let order;
  try {
    order = JSON.parse(msg.content.toString("utf8"));
    if (!cid || !validate(order)) throw new Error("invalid");
  } catch { return quarantine(msg); }

  const ctx = ctxOf(order);
  if (running.has(cid)) return ack(msg);
  let saga = store.sagas[cid];
  let fresh = false;

  if (saga) {
    if (OUTCOME_STATES.has(saga.state)) {
      await pubOutcome(saga.outcome.routingKey, saga.outcome.envelope);
      return ack(msg);
    }
    if (saga.state === "processing" && saga.owner !== RUN_ID && saga.steps.some((x) => x.status === "ok")) {
      await scanStuck();
      return ack(msg);
    }
  } else {
    const first = store.businessKeys[ctx.businessKey];
    if (first && first !== cid) {
      const env = envelope(ctx, cid, "duplicate", { duplicateOf: first });
      saga = store.sagas[cid] = { businessKey: ctx.businessKey, state: "duplicate", steps: [], compensations: [],
        outcome: { routingKey: "order.duplicate", envelope: env }, owner: RUN_ID, updatedAt: now() };
      persist();
      await pubOutcome("order.duplicate", env);
      return ack(msg);
    }
    store.businessKeys[ctx.businessKey] = cid;
    const env = envelope(ctx, cid, "processing");
    saga = store.sagas[cid] = { businessKey: ctx.businessKey, state: "processing", steps: [], compensations: [],
      outcome: { routingKey: "order.processing", envelope: env }, owner: RUN_ID, updatedAt: now() };
    fresh = true;
  }

  running.add(cid);
  try {
    const wasParked = saga.state === "parked";
    saga.owner = RUN_ID;
    if (fresh || wasParked) {
      const env = envelope(ctx, cid, "processing");
      saga.state = "processing"; saga.outcome = { routingKey: "order.processing", envelope: env }; saga.updatedAt = now();
      persist();
      await pubOutcome("order.processing", env);
    } else persist();
    await runSaga(msg, order, cid, saga, ctx, h);
  } finally { running.delete(cid); }
}

async function runSaga(msg, order, cid, saga, ctx, h) {
  const seq = order.orderType === "b2b" ? ["inventory", "shipping", "notification"]
    : ["payment", "inventory", "shipping", "notification"];
  for (let i = 0; i < seq.length; i++) {
    const step = seq[i];
    const r = await call(STEP[step](order, ctx.businessKey), cid);
    if (r.ok) { saga.steps.push({ step, status: "ok", at: now() }); persist(); continue; }

    if (i === 0) {
      if (r.temp) {
        const attempts = retryCount(h) + 1;
        if (attempts < MAX_RETRIES) return nack(msg);
        await confirmPub("orders.dlq.exchange", "", msg.content, { persistent: true, messageId: randomUUID(),
          headers: { ...h, originQueue: "saga.queue", lastError: r.code, causationId: randomUUID() } });
        await setOutcome(cid, saga, "order.parked", envelope(ctx, cid, "parked", { code: r.code }));
        return ack(msg);
      }
      saga.steps.push({ step, status: "failed", at: now(), code: r.code });
      await setOutcome(cid, saga, "order.failed", envelope(ctx, cid, "failed", { code: r.code }));
      return ack(msg);
    }

    saga.steps.push({ step, status: "failed", at: now(), code: r.code });
    persist();
    const c = await runComps(cid, saga, ctx);
    if (c.ok) await setOutcome(cid, saga, "order.compensated", envelope(ctx, cid, "compensated", { code: r.code }));
    else await setOutcome(cid, saga, "order.compensation_failed", envelope(ctx, cid, "compensation_failed", { code: c.code }));
    return ack(msg);
  }
  await setOutcome(cid, saga, "order.completed", envelope(ctx, cid, "completed"));
  ack(msg);
}

const app = express();
app.get("/health", (_req, res) => {
  const up = conn !== null && ch !== null && consuming;
  res.status(up ? 200 : 503).json({ status: up ? "ok" : "degraded",
    checks: { broker: up ? "up" : "down", consumers: { "saga.queue": consuming ? 1 : 0 } } });
});

app.get("/sagas/stuck", (_req, res) => {
  const sagas = Object.entries(store.sagas)
    .filter(([, s]) => s.state === "needs_attention" || s.state === "compensation_failed")
    .map(([cid, s]) => ({ correlationId: cid, businessKey: s.businessKey, state: s.state,
      completedSteps: s.steps.filter((x) => x.status === "ok").map((x) => x.step),
      compensatedSteps: s.compensations.filter((x) => x.status === "ok").map((x) => x.step),
      since: s.updatedAt }));
  res.json({ sagas });
});

app.post("/sagas/:cid/compensate", async (req, res) => {
  const cid = req.params.cid, s = store.sagas[cid];
  if (!s) return res.status(404).json({ code: "not_found" });
  if (compensating.has(cid)) return res.status(409).json({ code: "compensation_in_progress" });
  if (s.state === "compensated" && s.operatorCompensation) return res.json({ correlationId: cid, state: "compensated" });
  if (s.state !== "needs_attention" && s.state !== "compensation_failed")
    return res.status(409).json({ code: "saga_not_stuck" });
  compensating.add(cid);
  try {
    const prev = s.state, ctx = { ...s.outcome.envelope };
    const r = await runComps(cid, s, ctx);
    s.operatorCompensation = { at: now() };
    let state, code, rk;
    if (r.ok) {
      state = "compensated"; rk = "order.compensated";
      code = prev === "needs_attention" ? "operator_compensated"
        : (s.steps.find((x) => x.status === "failed")?.code ?? "operator_compensated");
    } else { state = "compensation_failed"; rk = "order.compensation_failed"; code = r.code; }
    await setOutcome(cid, s, rk, envelope(ctx, cid, state, { code }));
    res.json({ correlationId: cid, state });
  } catch (e) {
    console.error(e); res.status(500).json({ code: "internal_error" });
  } finally { compensating.delete(cid); }
});

app.listen(PORT, () => console.log(`orchestrator listening on ${PORT}`));
connect();
