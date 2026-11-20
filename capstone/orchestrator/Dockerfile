FROM node:20-bookworm-slim
WORKDIR /app
COPY orchestrator/package.json ./
RUN npm install && ln -s /app/node_modules /node_modules
COPY shared /shared
COPY orchestrator/src ./src
CMD ["node", "--import", "tsx", "src/server.ts"]
