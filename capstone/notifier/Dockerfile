FROM node:20-bookworm-slim
WORKDIR /app
COPY notifier/package*.json ./
RUN npm install --omit=dev
COPY notifier/ ./
CMD ["node", "server.js"]
