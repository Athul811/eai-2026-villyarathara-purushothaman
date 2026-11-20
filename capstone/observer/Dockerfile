FROM node:20-bookworm-slim
WORKDIR /app
COPY observer/package*.json ./
RUN npm install --omit=dev
COPY observer/ ./
CMD ["node", "server.js"]
