var amqp = require("amqplib/callback_api");

// Configuration: List of instance IDs to create queues for
var instanceIds = ["instanceId1", "instanceId2", "instanceId3"];

amqp.connect("amqp://admin:admin@localhost:5672", function (err, conn) {
  if (err) {
    console.error("Connection failed:", err);
    process.exit(1);
  }

  conn.createChannel(function (err, ch) {
    if (err) {
      console.error("Channel creation failed:", err);
      conn.close();
      process.exit(1);
    }

    // Send messages to each webhook queue
    instanceIds.forEach(function (instanceId, index) {
      var queueName = "webhook." + instanceId;
      var msg =
        "Webhook message for " + instanceId + " - " + new Date().toISOString();

      ch.assertQueue(queueName, { durable: false });
      ch.sendToQueue(queueName, Buffer.from(msg));
      console.log(" [x] Sent to %s: %s", queueName, msg);
    });

    setTimeout(function () {
      conn.close();
      process.exit(0);
    }, 500);
  });
});
