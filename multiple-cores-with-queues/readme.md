cd multiple-cores-with-queues
npm install

docker-compose up -d

3x
node worker-webhook-postgres.js

node submit-webhook-batch.js

node check-order.js # Check ordering per instance

node view-messages.js # View all messages
