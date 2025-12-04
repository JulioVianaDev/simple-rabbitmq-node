var amqp = require("amqplib/callback_api");
var http = require("http");

// Configuration
var QUEUE_PREFIX = "webhook.";
var RABBITMQ_URL = "amqp://admin:admin@localhost:5672";
var RABBITMQ_MGMT_HOST = "localhost";
var RABBITMQ_MGMT_PORT = 15672;
var RABBITMQ_USER = "admin";
var RABBITMQ_PASS = "admin";
var DISCOVERY_INTERVAL = 10000; // Poll every 10 seconds for new queues

// Track which queues we're already consuming from
var activeConsumers = {};
var connection = null;
var discoveryTimer = null;
var useManagementAPI = true; // Try Management API first

// Function to discover queues using RabbitMQ Management HTTP API
function discoverWebhookQueuesViaAPI(callback) {
  var auth = Buffer.from(RABBITMQ_USER + ":" + RABBITMQ_PASS).toString("base64");
  var path = "/api/queues";
  
  var options = {
    hostname: RABBITMQ_MGMT_HOST,
    port: RABBITMQ_MGMT_PORT,
    path: path,
    method: "GET",
    headers: {
      Authorization: "Basic " + auth,
    },
    timeout: 5000, // 5 second timeout
  };

  var req = http.request(options, function (res) {
    var data = "";

    res.on("data", function (chunk) {
      data += chunk;
    });

    res.on("end", function () {
      if (res.statusCode === 200) {
        try {
          var queues = JSON.parse(data);
          var webhookQueues = queues
            .filter(function (queue) {
              return queue.name && queue.name.startsWith(QUEUE_PREFIX);
            })
            .map(function (queue) {
              return queue.name;
            });
          callback(null, webhookQueues);
        } catch (err) {
          callback(err, null);
        }
      } else {
        callback(
          new Error(
            "Management API returned status code: " + res.statusCode
          ),
          null
        );
      }
    });
  });

  req.on("error", function (err) {
    callback(err, null);
  });

  req.on("timeout", function () {
    req.destroy();
    callback(new Error("Management API request timeout"), null);
  });

  req.end();
}

// Alternative: Try to discover queues by pattern matching on existing channels
// This is less efficient but works without Management API
function discoverWebhookQueuesViaChannels(callback) {
  if (!connection) {
    callback(new Error("No connection available"), null);
    return;
  }

  // Try to get queue info by attempting to bind/check queues
  // Note: This method requires knowing queue names in advance or using exchanges
  // For now, we'll return empty and rely on manual creation or Management API
  callback(null, []);
}

// Main discovery function with fallback
function discoverWebhookQueues(callback) {
  if (useManagementAPI) {
    discoverWebhookQueuesViaAPI(function (err, queues) {
      if (err) {
        console.log(
          "[*] Management API unavailable: %s. Will retry on next discovery cycle.",
          err.message
        );
        // Don't disable API completely, just log the error
        callback(err, null);
      } else {
        callback(null, queues);
      }
    });
  } else {
    discoverWebhookQueuesViaChannels(callback);
  }
}

// Function to create a consumer for a specific queue
function createConsumer(queueName) {
  // Skip if we're already consuming from this queue
  if (activeConsumers[queueName]) {
    return;
  }

  if (!connection) {
    console.error("No active connection available");
    return;
  }

  // Create a separate channel for each queue to maintain independent prefetch
  connection.createChannel(function (err, ch) {
    if (err) {
      console.error("Channel creation failed for " + queueName + ":", err);
      return;
    }

    ch.assertQueue(queueName, { durable: false }, function (err, ok) {
      if (err) {
        console.error("Queue assertion failed for " + queueName + ":", err);
        return;
      }

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

      // Mark this queue as active
      activeConsumers[queueName] = {
        channel: ch,
        queueName: queueName,
      };
    });
  });
}

// Function to discover and start consumers for all webhook queues
function discoverAndStartConsumers() {
  discoverWebhookQueues(function (err, queues) {
    if (err) {
      // Don't spam errors, just wait for next cycle
      return;
    }

    if (!queues || queues.length === 0) {
      return;
    }

    // Create consumers for newly discovered queues
    var newQueues = 0;
    queues.forEach(function (queueName) {
      if (!activeConsumers[queueName]) {
        console.log("[*] Starting consumer for: %s", queueName);
        createConsumer(queueName);
        newQueues++;
      }
    });

    if (newQueues > 0) {
      console.log(
        "[*] Started %d new consumer(s). Total active: %d\n",
        newQueues,
        Object.keys(activeConsumers).length
      );
    }
  });
}

// Connect to RabbitMQ
amqp.connect(RABBITMQ_URL, function (err, conn) {
  if (err) {
    console.error("Connection failed:", err);
    process.exit(1);
  }

  connection = conn;
  console.log("[*] Connected to RabbitMQ");
  console.log("[*] Auto-discovering webhook queues via Management API...");
  console.log(
    "[*] Management API: http://%s:%d/api/queues",
    RABBITMQ_MGMT_HOST,
    RABBITMQ_MGMT_PORT
  );

  // Initial discovery
  discoverAndStartConsumers();

  // Periodic discovery to catch newly created queues
  discoveryTimer = setInterval(function () {
    discoverAndStartConsumers();
  }, DISCOVERY_INTERVAL);

  console.log(
    "[*] Queue discovery will run every %d seconds\n",
    DISCOVERY_INTERVAL / 1000
  );
});

// Handle graceful shutdown
process.once("SIGINT", function () {
  console.log("\n[*] Shutting down...");
  
  if (discoveryTimer) {
    clearInterval(discoveryTimer);
  }
  
  // Close all channels
  Object.keys(activeConsumers).forEach(function (queueName) {
    var consumer = activeConsumers[queueName];
    if (consumer && consumer.channel) {
      consumer.channel.close();
    }
  });
  
  if (connection) {
    connection.close();
  }
  
  process.exit(0);
});

