import { createService } from "../../shared/service";
const { broker, start } = createService();
broker.consume("trace.store", (msg, ch) => ch.ack(msg));
start();
