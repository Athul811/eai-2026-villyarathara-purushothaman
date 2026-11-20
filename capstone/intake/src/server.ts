import { createService } from "../../shared/service";
import { randomUUID } from "node:crypto";

const { app, broker, start } = createService();

const orders = new Map<string, unknown>();

app.use(express.json());