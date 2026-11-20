import { createService } from "../../shared/service";
const { broker, start } = createService();
broker.consume("saga.queue", (msg, ch) => ch.ack(msg), Number(process.env.SAGA_CONCURRENCY ?? 5));
start();
