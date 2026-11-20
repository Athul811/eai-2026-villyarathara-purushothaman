import { randomUUID } from "node:crypto";

let chan = null;

// A dedicated, non-confirm channel: publishing a hop never blocks or fails the business operation (R-4.8.2).
export async function attachTrace(conn) {
  const c = await conn.createChannel();
  c.on("error", () => {});
  c.on("close", () => { if (chan === c) chan = null; });
  chan = c;
}

export function makeHop({ service, operation, status = "success", correlationId, causationId = null,
  attempt = 1, messageId, startedAt, hopId, detail }) {
  const finished = new Date();
  const started = startedAt ? new Date(startedAt) : finished;
  return {
    hopId: hopId || randomUUID(), correlationId, causationId: causationId ?? null, service, operation, status, attempt,
    ...(messageId ? { messageId } : {}),
    startedAt: started.toISOString(), finishedAt: finished.toISOString(),
    durationMs: finished.getTime() - started.getTime(),
    ...(detail ? { detail } : {}),
  };
}

export function emitHop(h) {
  try {
    chan?.publish("trace.events", "", Buffer.from(JSON.stringify(h)),
      { persistent: true, contentType: "application/json", messageId: randomUUID() });
  } catch { /* a lost hop never fails the business operation */ }
}
