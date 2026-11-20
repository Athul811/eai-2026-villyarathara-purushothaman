FROM node:20-bookworm-slim
WORKDIR /app
COPY orchestrator/package*.json ./
RUN npm install --omit=dev
COPY orchestrator/ ./
CMD ["node", "server.js"]
