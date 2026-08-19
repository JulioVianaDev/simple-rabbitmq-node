var amqp = require("amqplib/callback_api");

// Configuration: List of instance IDs to create queues for
var instanceIds = [
  "instanceId1",
  "instanceId2",
  "instanceId3",
  "instanceId4",
  "instanceId5",
  "instanceId6",
  "instanceId7",
  "instanceId8",
  "instanceId9",
  "instanceId10",
];
var messagesPerQueue = 15; // Number of messages to send to each queue

amqp.connect("amqp://admin:admin@localhost:5675", function (err, conn) {
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

    var totalMessages = 0;

    // Send multiple messages to each webhook queue concurrently
    var promises = instanceIds.map(function (instanceId) {
      return new Promise(function (resolve, reject) {
        var queueName = "webhook." + instanceId;

        ch.assertQueue(queueName, { durable: false }, function (err) {
          if (err) {
            console.error("Queue assertion failed for " + queueName + ":", err);
            reject(err);
            return;
          }

          // Send multiple messages to demonstrate ordering
          // Include sequence number in message for ordering verification
          for (var i = 1; i <= messagesPerQueue; i++) {
            var messageObj = {
              instanceId: instanceId,
              sequence: i,
              message: "Message #" + i + " for " + instanceId,
              timestamp: new Date().toISOString(),
              sentAt: Date.now(),
            };
            var msg = JSON.stringify(messageObj);
            ch.sendToQueue(queueName, Buffer.from(msg));
            console.log(" [x] Sent to %s: Sequence #%d", queueName, i);
            totalMessages++;
          }
          resolve();
        });
      });
    });

    // Execute all instanceIds concurrently
    Promise.all(promises)
      .then(function () {
        console.log(
          "\n[*] All queues processed. Total messages sent: %d",
          totalMessages
        );
        conn.close();
        process.exit(0);
      })
      .catch(function (err) {
        console.error("Error processing queues:", err);
        conn.close();
        process.exit(1);
      });
  });
});
