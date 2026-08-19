var amqp = require("amqplib/callback_api");
var http = require("http");
var { Client } = require("pg");
var crypto = require("crypto");

// Configuration
var QUEUE_PREFIX = "webhook.";
var RABBITMQ_URL = "amqp://admin:admin@localhost:5675";
var RABBITMQ_MGMT_HOST = "localhost";
var RABBITMQ_MGMT_PORT = 15675;
var RABBITMQ_USER = "admin";
var RABBITMQ_PASS = "admin";
var DISCOVERY_INTERVAL = 10000; // Poll every 10 seconds for new queues

// PostgreSQL Configuration (only for logging messages, not for coordination)
var PG_CONFIG = {
  host: "localhost",
  port: 5432,
  database: "webhook_messages",
  user: "admin",
  password: "admin",
};

// Generate deterministic worker ID based on process info
// This allows workers to have consistent IDs across restarts on the same machine
var os = require("os");
var processId = process.pid;
var hostname = os.hostname();
var WORKER_ID = "worker-" + hostname + "-" + processId;

// Track which queues we're currently consuming from
var activeConsumers = {};
var connection = null;
var discoveryTimer = null;
var heartbeatTimer = null;
var pgClient = null;

// Simple hash function for consistent queue assignment
function hashString(str) {
  return crypto.createHash("md5").update(str).digest("hex");
}

// Convert hash to integer for modulo operation
function hashToInt(str) {
  var hash = hashString(str);
  // Take first 8 characters and convert to integer
  return parseInt(hash.substring(0, 8), 16);
}

// Initialize PostgreSQL connection (only for logging)
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

// Function to make HTTP request to RabbitMQ Management API
function makeManagementRequest(path, callback) {
  var auth = Buffer.from(RABBITMQ_USER + ":" + RABBITMQ_PASS).toString(
    "base64"
  );

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
          var result = JSON.parse(data);
          callback(null, result);
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

// Discover all webhook queues
function discoverWebhookQueues(callback) {
  makeManagementRequest("/api/queues", function (err, queues) {
    if (err) {
      callback(err, null);
      return;
    }

    var webhookQueues = queues
      .filter(function (queue) {
        return queue.name && queue.name.startsWith(QUEUE_PREFIX);
      })
      .map(function (queue) {
        return queue.name;
      })
      .sort(); // Sort for consistent ordering

    callback(null, webhookQueues);
  });
}

// Get consumers for all queues (to discover active workers)
function getQueueConsumers(callback) {
  makeManagementRequest("/api/queues", function (err, queues) {
    if (err) {
      callback(err, null);
      return;
    }

    var queueConsumers = {};
    var allWorkers = new Set();

    // Get consumers for each webhook queue
    var webhookQueues = queues.filter(function (queue) {
      return queue.name && queue.name.startsWith(QUEUE_PREFIX);
    });

    // Process each queue to get its consumers
    var processed = 0;
    if (webhookQueues.length === 0) {
      callback(null, { queueConsumers: {}, allWorkers: [] });
      return;
    }

    webhookQueues.forEach(function (queue) {
      var queueName = queue.name;
      var vhost = encodeURIComponent(queue.vhost || "/");
      var path = "/api/queues/" + vhost + "/" + encodeURIComponent(queueName);

      makeManagementRequest(path, function (err, queueDetails) {
        processed++;

        if (!err && queueDetails && queueDetails.consumer_details) {
          queueConsumers[queueName] = queueDetails.consumer_details.map(
            function (consumer) {
              // Extract worker ID from consumer tag (format: "worker-{id}")
              var consumerTag = consumer.consumer_tag || "";
              var workerId = consumerTag.replace(/^amq\.ctag-/, "");

              // Try to extract worker ID from consumer tag if it contains our pattern
              // Consumer tags from amqplib are like "amq.ctag-xxxxx"
              // We'll use the channel details or connection name if available
              if (consumer.channel_details && consumer.channel_details.name) {
                // Channel name might contain worker info
                var channelName = consumer.channel_details.name;
                // Look for worker pattern in connection name
                if (
                  consumer.channel_details.connection_name &&
                  consumer.channel_details.connection_name.includes("worker-")
                ) {
                  var match =
                    consumer.channel_details.connection_name.match(
                      /worker-[^\s]+/
                    );
                  if (match) {
                    workerId = match[0];
                  }
                }
              }

              // If we can't extract worker ID, use consumer tag as fallback
              if (!workerId || workerId.startsWith("amq.ctag-")) {
                workerId = consumerTag;
              }

              if (workerId && workerId.includes("worker-")) {
                allWorkers.add(workerId);
              }

              return {
                consumerTag: consumerTag,
                workerId: workerId,
              };
            }
          );
        } else {
          queueConsumers[queueName] = [];
        }

        // When all queues are processed
        if (processed === webhookQueues.length) {
          callback(null, {
            queueConsumers: queueConsumers,
            allWorkers: Array.from(allWorkers).sort(),
          });
        }
      });
    });
  });
}

// Coordination queue for worker discovery
var COORDINATION_QUEUE = "worker-coordination-discovery";

// Publish our worker ID to coordination queue (called periodically)
var coordinationChannel = null;
function publishWorkerHeartbeat() {
  if (!connection) return;

  if (!coordinationChannel) {
    connection.createChannel(function (err, ch) {
      if (err) return;
      coordinationChannel = ch;
      ch.assertQueue(
        COORDINATION_QUEUE,
        {
          durable: false,
          arguments: {
            "x-message-ttl": 30000, // Messages expire after 30 seconds
          },
        },
        function (err) {
          if (err) {
            coordinationChannel = null;
            return;
          }
          sendHeartbeatMessage();
        }
      );
    });
  } else {
    sendHeartbeatMessage();
  }
}

function sendHeartbeatMessage() {
  if (!coordinationChannel) return;

  var workerInfo = {
    workerId: WORKER_ID,
    timestamp: Date.now(),
  };

  coordinationChannel.sendToQueue(
    COORDINATION_QUEUE,
    Buffer.from(JSON.stringify(workerInfo)),
    { persistent: false }
  );
}

// Get active workers by reading coordination queue via Management API
function getActiveWorkers(callback) {
  var vhost = encodeURIComponent("/");
  var path =
    "/api/queues/" + vhost + "/" + encodeURIComponent(COORDINATION_QUEUE);

  // First, ensure coordination queue exists and publish our heartbeat
  publishWorkerHeartbeat();

  // Get queue details to see message count
  makeManagementRequest(path, function (err, queueInfo) {
    if (err || !queueInfo) {
      // Queue doesn't exist yet or error - assume we're the only worker
      callback(null, [WORKER_ID]);
      return;
    }

    var messageCount = queueInfo.messages || 0;

    if (messageCount === 0) {
      callback(null, [WORKER_ID]);
      return;
    }

    // Use Management API to get messages from queue (this consumes them, but that's ok
    // since we republish heartbeats periodically)
    // Actually, we can't get messages via Management API without consuming
    // So we'll use a different approach: read via AMQP but republish

    if (!connection) {
      callback(null, [WORKER_ID]);
      return;
    }

    connection.createChannel(function (err, ch) {
      if (err) {
        callback(null, [WORKER_ID]);
        return;
      }

      var discoveredWorkers = new Set();
      discoveredWorkers.add(WORKER_ID); // Always include ourselves

      var messagesRead = 0;
      var messagesToRead = Math.min(messageCount, 100); // Limit to 100 to avoid timeout

      // Read messages and republish them
      function readNextMessage() {
        ch.get(COORDINATION_QUEUE, { noAck: false }, function (err, msg) {
          if (err || !msg) {
            // No more messages or error
            ch.close();
            var workers = Array.from(discoveredWorkers).sort();
            if (workers.indexOf(WORKER_ID) === -1) {
              workers.push(WORKER_ID);
              workers.sort();
            }
            callback(null, workers);
            return;
          }

          try {
            var workerInfo = JSON.parse(msg.content.toString());
            if (
              workerInfo.workerId &&
              workerInfo.workerId.startsWith("worker-")
            ) {
              discoveredWorkers.add(workerInfo.workerId);
              // Republish so other workers can see it
              ch.sendToQueue(COORDINATION_QUEUE, msg.content, {
                persistent: false,
              });
            }
            ch.ack(msg);
            messagesRead++;

            if (messagesRead < messagesToRead) {
              readNextMessage();
            } else {
              ch.close();
              var workers = Array.from(discoveredWorkers).sort();
              if (workers.indexOf(WORKER_ID) === -1) {
                workers.push(WORKER_ID);
                workers.sort();
              }
              callback(null, workers);
            }
          } catch (e) {
            ch.ack(msg);
            messagesRead++;
            if (messagesRead < messagesToRead) {
              readNextMessage();
            } else {
              ch.close();
              var workers = Array.from(discoveredWorkers).sort();
              if (workers.indexOf(WORKER_ID) === -1) {
                workers.push(WORKER_ID);
                workers.sort();
              }
              callback(null, workers);
            }
          }
        });
      }

      // Ensure queue exists
      ch.assertQueue(COORDINATION_QUEUE, { durable: false }, function (err) {
        if (err) {
          ch.close();
          callback(null, [WORKER_ID]);
          return;
        }
        readNextMessage();
      });
    });
  });
}

// Check if this worker should handle a specific queue
function shouldHandleQueue(queueName, totalWorkers, myIndex) {
  if (totalWorkers === 0) {
    return false;
  }

  // Use consistent hashing: hash(queueName) % totalWorkers == myIndex
  var hashValue = hashToInt(queueName);
  var assignedWorker = hashValue % totalWorkers;

  return assignedWorker === myIndex;
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

      // Use worker ID in consumer tag for identification
      var consumerTag = WORKER_ID;

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

            // Simulate processing time (you can replace this with your actual processing logic)
            setTimeout(function () {
              // Acknowledge the message after processing
              // This allows the next message from this queue to be processed
              console.log(" [✓] Processed message from %s", receivedQueue);
              ch.ack(msg);
            }, 1000); // Simulate 1 second processing time
          }
        },
        {
          noAck: false, // Set to false to manually acknowledge messages
          consumerTag: consumerTag, // Use worker ID as consumer tag
        }
      );

      // Mark this queue as active
      activeConsumers[queueName] = {
        channel: ch,
        queueName: queueName,
        consumerTag: consumerTag,
      };
    });
  });
}

// Function to stop consuming from a queue
function stopConsumer(queueName) {
  if (!activeConsumers[queueName]) {
    return;
  }

  var consumer = activeConsumers[queueName];
  if (consumer && consumer.channel) {
    console.log("[*] Stopping consumer for: %s", queueName);
    consumer.channel.close(function (err) {
      if (err) {
        console.error(
          "[!] Error closing channel for " + queueName + ":",
          err.message
        );
      }
    });
    delete activeConsumers[queueName];
  }
}

// Function to discover and balance consumers across workers
function discoverAndBalanceConsumers() {
  // First, get active workers from RabbitMQ
  getActiveWorkers(function (err, workers) {
    if (err) {
      console.error("[!] Failed to get active workers:", err.message);
      return;
    }

    var totalWorkers = workers.length;

    if (totalWorkers === 0) {
      console.log("[!] No active workers found. Waiting...");
      return;
    }

    // Find our worker index
    var myIndex = workers.indexOf(WORKER_ID);

    if (myIndex === -1) {
      console.log("[!] This worker not found in active workers list");
      return;
    }

    console.log(
      "[*] Active workers: %d, My index: %d, My ID: %s",
      totalWorkers,
      myIndex,
      WORKER_ID
    );

    // Discover all webhook queues
    discoverWebhookQueues(function (err, queues) {
      if (err) {
        console.error(
          "[!] Failed to discover queues via Management API:",
          err.message
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
        // Stop all consumers if no queues exist
        Object.keys(activeConsumers).forEach(function (queueName) {
          stopConsumer(queueName);
        });
        return;
      }

      // Get current consumers for all queues
      getQueueConsumers(function (err, result) {
        if (err) {
          console.error("[!] Failed to get queue consumers:", err.message);
          // Continue with assignment anyway
        }

        var queueConsumers = result ? result.queueConsumers : {};

        // Calculate which queues we should handle based on consistent hashing
        var queuesToHandle = queues.filter(function (queueName) {
          // Check if queue should be assigned to us by hash
          return shouldHandleQueue(queueName, totalWorkers, myIndex);
        });

        console.log(
          "[*] Total queues: %d, Assigned to this worker: %d",
          queues.length,
          queuesToHandle.length
        );

        // Start consumers for queues we should handle but aren't yet
        queuesToHandle.forEach(function (queueName) {
          if (!activeConsumers[queueName]) {
            // Check if queue already has a consumer
            var queueInfo = queueConsumers[queueName];
            var hasConsumer = queueInfo && queueInfo.hasConsumer;

            // Only start if queue has no consumer OR if we're already consuming (reconnect scenario)
            if (!hasConsumer) {
              console.log("[*] Starting consumer for: %s", queueName);
              createConsumer(queueName);
            } else {
              console.log(
                "[*] Queue %s already has a consumer, waiting for rebalance...",
                queueName
              );
            }
          }
        });

        // Stop consumers for queues we shouldn't handle anymore
        Object.keys(activeConsumers).forEach(function (queueName) {
          if (!queuesToHandle.includes(queueName)) {
            console.log("[*] Stopping consumer for %s (rebalanced)", queueName);
            stopConsumer(queueName);
          }
        });

        if (queuesToHandle.length > 0) {
          console.log(
            "[*] Currently handling queues: %s\n",
            queuesToHandle.join(", ")
          );
        }
      });
    });
  });
}

// Initialize PostgreSQL first (only for logging), then connect to RabbitMQ
console.log("[*] Initializing PostgreSQL connection (for logging only)...");
initPostgreSQL(function () {
  // Connect to RabbitMQ
  // Note: We'll identify workers by checking which queues they consume from
  // and by using consistent hashing based on worker process info
  amqp.connect(RABBITMQ_URL, function (err, conn) {
    if (err) {
      console.error("Connection failed:", err);
      process.exit(1);
    }

    connection = conn;
    console.log("[*] Connected to RabbitMQ");
    console.log("[*] Worker ID: %s", WORKER_ID);
    console.log(
      "[*] Using RabbitMQ Management API for coordination (no database needed)"
    );
    console.log(
      "[*] Management API: http://%s:%d/api/queues",
      RABBITMQ_MGMT_HOST,
      RABBITMQ_MGMT_PORT
    );

    // Initial discovery and balancing
    console.log("\n[*] Discovering webhook queues and balancing...");
    discoverAndBalanceConsumers();

    // Periodic discovery to catch newly created queues and rebalance
    discoveryTimer = setInterval(function () {
      discoverAndBalanceConsumers();
    }, DISCOVERY_INTERVAL);

    // Periodic heartbeat to coordination queue (every 5 seconds)
    var heartbeatTimer = setInterval(function () {
      publishWorkerHeartbeat();
    }, 5000);

    // Initial heartbeat
    setTimeout(function () {
      publishWorkerHeartbeat();
    }, 1000);

    console.log(
      "[*] Queue discovery/balancing will run every %d seconds",
      DISCOVERY_INTERVAL / 1000
    );
    console.log("[*] Worker heartbeat will run every 5 seconds\n");
  });
});

// Handle graceful shutdown
process.once("SIGINT", function () {
  console.log("\n[*] Shutting down...");

  if (discoveryTimer) {
    clearInterval(discoveryTimer);
  }

  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
  }

  if (coordinationChannel) {
    coordinationChannel.close();
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
