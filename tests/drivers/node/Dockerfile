FROM node:20-alpine

WORKDIR /tests
COPY package.json ./
RUN npm install --production

COPY test_cql.js ./

CMD ["node", "test_cql.js"]
