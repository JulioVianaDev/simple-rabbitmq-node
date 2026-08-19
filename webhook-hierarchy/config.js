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
  // Raw queue (API→Router):  webhooks.raw.{instanceId}
  // Customer queue:          {tenantId}.webhooks.{instanceId}.{customerId}
  // System queue:            {tenantId}.webhooks.{instanceId}.__system__

  DISCOVERY_INTERVAL: 10000,
  HEARTBEAT_INTERVAL: 5000,
};
