FROM node:20-bookworm-slim
WORKDIR /app
COPY intake/package.json ./
RUN npm install && ln -s /app/node_modules /node_modules
COPY shared /shared
COPY intake/src ./src
CMD ["node", "--import", "tsx", "src/server.ts"]
