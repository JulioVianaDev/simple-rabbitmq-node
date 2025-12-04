var amqp = require("amqplib/callback_api");

// Configuration: List of instance IDs to consume from
// You can modify this list or make it dynamic (e.g., from config file, environment variable, or discover from RabbitMQ)
var instanceIds = ["instanceId1", "instanceId2", "instanceId3"];

amqp.connect("amqp://admin:admin@localhost:5672", function (err, conn) {
  if (err) {
    console.error("Connection failed:", err);
    process.exit(1);
  }

  var connectedQueues = 0;
  var totalQueues = instanceIds.length;

  // Create consumers for each webhook queue
  instanceIds.forEach(function (instanceId) {
    var queueName = "webhook." + instanceId;

    // Create a separate channel for each queue to maintain independent prefetch
    conn.createChannel(function (err, ch) {
      if (err) {
        console.error("Channel creation failed for " + queueName + ":", err);
        return;
      }

      ch.assertQueue(queueName, { durable: false });

      // Prefetch 1 ensures only 1 unacknowledged message per queue at a time
      // This maintains message order within each queue
      ch.prefetch(1);

      console.log(
        " [*] Waiting for messages in %s. To exit press CTRL+C",
        queueName
      );

      ch.consume(
        queueName,
        function (msg) {
          if (msg) {
            var messageContent = msg.content.toString();
            var receivedQueue = queueName;

            console.log(
              " [x] Received from %s: %s",
              receivedQueue,
              messageContent
            );

            // Simulate processing time (you can replace this with your actual processing logic)
            setTimeout(function () {
              // Acknowledge the message after processing
              // This allows the next message from this queue to be processed
              ch.ack(msg);
              console.log(" [✓] Processed message from %s", receivedQueue);
            }, 1000); // Simulate 1 second processing time
          }
        },
        { noAck: false } // Set to false to manually acknowledge messages
      );

      connectedQueues++;
      if (connectedQueues === totalQueues) {
        console.log(
          "\n[*] All %d webhook queues are connected and ready\n",
          totalQueues
        );
      }
    });
  });
});
