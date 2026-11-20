FROM node:20-bookworm-slim
WORKDIR /app
COPY intake/package*.json ./
RUN npm install --omit=dev
COPY intake/ ./
CMD ["node", "server.js"]
