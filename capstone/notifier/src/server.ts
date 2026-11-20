import { createService } from "../../shared/service";
const { broker, start } = createService();
broker.consume("notifications.queue", (msg, ch) => ch.ack(msg));
start();
