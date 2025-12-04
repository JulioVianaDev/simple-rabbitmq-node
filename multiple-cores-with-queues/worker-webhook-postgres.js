var amqp = require("amqplib/callback_api");
var http = require("http");
var { Client } = require("pg");

// Configuration
var QUEUE_PREFIX = "webhook.";
var RABBITMQ_URL = "amqp://admin:admin@localhost:5672";
var RABBITMQ_MGMT_HOST = "localhost";
var RABBITMQ_MGMT_PORT = 15672;
var RABBITMQ_USER = "admin";
var RABBITMQ_PASS = "admin";
var DISCOVERY_INTERVAL = 10000; // Poll every 10 seconds for new queues

// PostgreSQL Configuration
var PG_CONFIG = {
  host: "localhost",
  port: 5432,
  database: "webhook_messages",
  user: "admin",
  password: "admin",
};

// Generate unique worker ID
var WORKER_ID =
  "worker-" + Date.now() + "-" + Math.random().toString(36).substr(2, 9);

// Track which queues we're already consuming from
var activeConsumers = {};
var connection = null;
var discoveryTimer = null;
var pgClient = null;

// Initialize PostgreSQL connection
function initPostgreSQL(callback) {
  pgClient = new Client(PG_CONFIG);

  pgClient.connect(function (err) {
    if (err) {
      console.error("PostgreSQL connection failed:", err.message);
      console.log("[!] Retrying PostgreSQL connection in 5 seconds...");
      setTimeout(function () {
        initPostgreSQL(callback);
      }, 5000);
      return;
    }

    console.log("[*] Connected to PostgreSQL");
    if (callback) callback();
  });

  pgClient.on("error", function (err) {
    console.error("PostgreSQL connection error:", err.message);
    console.log("[!] Attempting to reconnect...");
    setTimeout(function () {
      initPostgreSQL();
    }, 5000);
  });
}

// Function to log message to PostgreSQL
function logMessageToPostgreSQL(queueName, messageContent, callback) {
  if (!pgClient) {
    console.error("[!] PostgreSQL client not available");
    if (callback) callback(new Error("PostgreSQL client not available"));
    return;
  }

  var instanceId = queueName.replace(QUEUE_PREFIX, "");
  var parsedMessage = null;
  var sequence = null;

  // Try to parse JSON message to extract sequence number
  try {
    parsedMessage = JSON.parse(messageContent);
    sequence = parsedMessage.sequence || null;
  } catch (e) {
    // Not JSON, use message as-is
    parsedMessage = { content: messageContent };
  }

  var consumedAt = new Date();

  var query =
    "INSERT INTO consumed_messages (instance_id, queue_name, message_content, message_sequence, consumed_at, worker_id) VALUES ($1, $2, $3, $4, $5, $6)";
  var values = [
    instanceId,
    queueName,
    messageContent,
    sequence,
    consumedAt,
    WORKER_ID,
  ];

  pgClient.query(query, values, function (err, result) {
    if (err) {
      console.error("[!] Failed to log message to PostgreSQL:", err.message);
      if (callback) callback(err);
      return;
    }

    if (callback) callback(null, result);
  });
}

// Function to discover queues using RabbitMQ Management HTTP API
function discoverWebhookQueues(callback) {
  var auth = Buffer.from(RABBITMQ_USER + ":" + RABBITMQ_PASS).toString(
    "base64"
  );
  var path = "/api/queues";

  var options = {
    hostname: RABBITMQ_MGMT_HOST,
    port: RABBITMQ_MGMT_PORT,
    path: path,
    method: "GET",
    headers: {
      Authorization: "Basic " + auth,
    },
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
          new Error("Management API returned status code: " + res.statusCode),
          null
        );
      }
    });
  });

  req.on("error", function (err) {
    callback(err, null);
  });

  req.end();
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
            var processedAt = new Date();

            console.log(
              " [x] Received from %s: %s",
              receivedQueue,
              messageContent.substring(0, 100) +
                (messageContent.length > 100 ? "..." : "")
            );

            // Log to PostgreSQL BEFORE processing
            logMessageToPostgreSQL(
              receivedQueue,
              messageContent,
              function (err) {
                if (err) {
                  console.error(
                    "[!] Failed to log message to database:",
                    err.message
                  );
                }
              }
            );

            ch.ack(msg);
            console.log(" [✓] Processed message from %s", receivedQueue);
            // Simulate processing time (you can replace this with your actual processing logic)
            // setTimeout(function () {
            //   // Acknowledge the message after processing
            //   // This allows the next message from this queue to be processed
            //   console.log(" [✓] Processed message from %s", receivedQueue);
            //   ch.ack(msg);
            // }, 1000); // Simulate 1 second processing time
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
      console.error(
        "[!] Failed to discover queues via Management API:",
        err.message
      );
      console.log(
        "[*] Make sure RabbitMQ Management Plugin is enabled and accessible at http://%s:%d",
        RABBITMQ_MGMT_HOST,
        RABBITMQ_MGMT_PORT
      );
      return;
    }

    if (!queues || queues.length === 0) {
      if (Object.keys(activeConsumers).length === 0) {
        console.log(
          "[*] No queues found with prefix '%s'. Waiting for queues to be created...",
          QUEUE_PREFIX
        );
      }
      return;
    }

    var newQueues = [];

    // Create consumers for newly discovered queues
    queues.forEach(function (queueName) {
      if (!activeConsumers[queueName]) {
        console.log("[*] Starting consumer for: %s", queueName);
        createConsumer(queueName);
        newQueues.push(queueName);
      }
    });

    if (newQueues.length > 0) {
      console.log(
        "[*] Started %d new consumer(s) for: %s",
        newQueues.length,
        newQueues.join(", ")
      );
    }

    var totalActive = Object.keys(activeConsumers).length;
    if (totalActive > 0 && newQueues.length > 0) {
      console.log("[*] Total active consumers: %d\n", totalActive);
    }
  });
}

// Initialize PostgreSQL first, then connect to RabbitMQ
console.log("[*] Initializing PostgreSQL connection...");
initPostgreSQL(function () {
  // Connect to RabbitMQ
  amqp.connect(RABBITMQ_URL, function (err, conn) {
    if (err) {
      console.error("Connection failed:", err);
      process.exit(1);
    }

    connection = conn;
    console.log("[*] Connected to RabbitMQ");
    console.log("[*] Worker ID: %s", WORKER_ID);
    console.log("[*] Auto-discovering webhook queues via Management API...");
    console.log(
      "[*] Management API: http://%s:%d/api/queues",
      RABBITMQ_MGMT_HOST,
      RABBITMQ_MGMT_PORT
    );

    // Initial discovery
    console.log("\n[*] Discovering webhook queues...");
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

  if (pgClient) {
    pgClient.end();
  }

  process.exit(0);
});
