module.exports = {
  RABBITMQ_URL: "amqp://admin:admin@localhost:5672",
  RABBITMQ_MGMT_HOST: "localhost",
  RABBITMQ_MGMT_PORT: 15672,
  RABBITMQ_USER: "admin",
  RABBITMQ_PASS: "admin",

  PG_CONFIG: {
    host: "localhost",
    port: 5432,
    database: "webhook_messages",
    user: "admin",
    password: "admin",
  },

  // Queue naming convention:
  // Raw queue (API->Router):  webhooks.raw.{instanceId}
  // Customer queue:           {tenantId}.webhooks.{instanceId}.{customerId}
  // System queue:             {tenantId}.webhooks.{instanceId}.__system__

  // Discovery is now just a FALLBACK for rebalancing and cleanup.
  // New queues are picked up instantly via the notification queue.
  DISCOVERY_INTERVAL: 30000,
  HEARTBEAT_INTERVAL: 5000,

  // Router notifies processors about new customer queues via a fanout exchange.
  // Fanout = every processor worker receives every notification.
  // Each worker checks consistent hashing to decide if the queue is theirs.
  NOTIFY_EXCHANGE: "processor-new-queue-notify",

  // Max customer queues a single processor worker will consume at once.
  // Each consumer = 1 channel = ~50-100KB RAM.
  // 500 consumers ≈ 25-50MB RAM just for channels.
  MAX_CONSUMERS_PER_WORKER: 500,

  // Customer queues auto-delete after this many ms with no consumers AND no messages.
  // RabbitMQ's x-expires: queue is deleted if unused (no consumers) for this duration.
  // 5 minutes = 300000ms
  CUSTOMER_QUEUE_EXPIRES_MS: 30000,
};
