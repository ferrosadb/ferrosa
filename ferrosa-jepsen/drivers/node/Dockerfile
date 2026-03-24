FROM node:20-slim
WORKDIR /app
COPY package.json .
RUN npm install --production
COPY workload.js .
ENTRYPOINT ["node", "workload.js"]
