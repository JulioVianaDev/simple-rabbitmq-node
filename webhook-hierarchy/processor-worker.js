var amqp = require("amqplib/callback_api");
var http = require("http");
var { Client } = require("pg");
var crypto = require("crypto");
var os = require("os");
var config = require("./config");

// ============================================================
// PROCESSOR WORKER
// Consumes from customer queues: {tenantId}.webhooks.{instanceId}.{customerId}
// Also consumes from system queues: {tenantId}.webhooks.{instanceId}.__system__
//
// KEY GUARANTEES:
// - Each customer queue has prefetch(1) → messages for the same customer
//   are processed ONE AT A TIME, IN ORDER
// - But MANY customer queues are consumed IN PARALLEL (one channel per queue)
// - Multiple processor workers split customer queues via consistent hashing
//   → no two workers consume the same customer queue
// - Workers auto-discover new customer queues and rebalance
//
// Every processed message is logged to PostgreSQL for testing:
// - table: processed_messages
// - view:  v_order_check (verify ordering per customer)
// - view:  v_worker_distribution (which worker handled which queues)
// ============================================================

var WORKER_ID = "processor-" + os.hostname() + "-" + process.pid;
var connection = null;
var pgClient = null;
var activeConsumers = {};
var shuttingDown = false;
var inFlightCount = 0;

// --- PostgreSQL ---
function initPostgres(callback) {
  pgClient = new Client(config.PG_CONFIG);
  pgClient.connect(function (err) {
    if (err) {
      console.error("[!] PG connection failed:", err.message);
      setTimeout(function () { initPostgres(callback); }, 3000);
      return;
    }
    console.log("[*] Connected to PostgreSQL");
    if (callback) callback();
  });

  pgClient.on("error", function (err) {
    console.error("[!] PG error:", err.message);
    setTimeout(function () { initPostgres(); }, 3000);
  });
}

function logProcessedMessage(queueName, webhookData, callback) {
  if (!pgClient) {
    if (callback) callback(new Error("PG not connected"));
    return;
  }

  var parts = queueName.split(".");
  var tenantId = parts[0];
  var instanceId = parts[2];
  var customerId = parts[3];

  var query = "INSERT INTO processed_messages " +
    "(tenant_id, instance_id, customer_id, queue_name, event, body, sequence, webhook_received_at, worker_id) " +
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

  var values = [
    tenantId,
    instanceId,
    customerId,
    queueName,
    webhookData.event || null,
    webhookData.body || null,
    webhookData.sequence || null,
    webhookData.receivedAt || null,
    WORKER_ID,
  ];

  pgClient.query(query, values, function (err) {
    if (err) {
      console.error("[!] Failed to log message:", err.message);
    }
    if (callback) callback(err);
  });
}

// --- Hashing ---
function hashToInt(str) {
  var hash = crypto.createHash("md5").update(str).digest("hex");
  return parseInt(hash.substring(0, 8), 16);
}

// --- Management API ---
function mgmtRequest(path, callback) {
  var auth = Buffer.from(config.RABBITMQ_USER + ":" + config.RABBITMQ_PASS).toString("base64");
  var req = http.request({
    hostname: config.RABBITMQ_MGMT_HOST,
    port: config.RABBITMQ_MGMT_PORT,
    path: path,
    method: "GET",
    headers: { Authorization: "Basic " + auth },
  }, function (res) {
    var data = "";
    res.on("data", function (chunk) { data += chunk; });
    res.on("end", function () {
      if (res.statusCode === 200) {
        try { callback(null, JSON.parse(data)); } catch (e) { callback(e); }
      } else {
        callback(new Error("MGMT API status " + res.statusCode));
      }
    });
  });
  req.on("error", callback);
  req.end();
}

// --- Worker coordination ---
var COORD_QUEUE = "processor-coordination";
var coordChannel = null;

function publishHeartbeat() {
  if (!connection) return;
  function send() {
    if (!coordChannel) return;
    coordChannel.sendToQueue(COORD_QUEUE, Buffer.from(JSON.stringify({
      workerId: WORKER_ID, timestamp: Date.now(),
    })), { persistent: false });
  }
  if (!coordChannel) {
    connection.createChannel(function (err, ch) {
      if (err) return;
      coordChannel = ch;
      ch.on("error", function (err) {
        console.error("[!] Coord channel error:", err.message);
        coordChannel = null;
      });
      ch.assertQueue(COORD_QUEUE, { durable: false, arguments: { "x-message-ttl": 30000 } }, function (err) {
        if (err) { coordChannel = null; return; }
        send();
      });
    });
  } else {
    send();
  }
}

function getActiveProcessors(callback) {
  publishHeartbeat();
  if (!connection) { callback(null, [WORKER_ID]); return; }

  connection.createChannel(function (err, ch) {
    if (err) { callback(null, [WORKER_ID]); return; }

    ch.on("error", function (err) {
      console.error("[!] Coordination channel error:", err.message);
    });

    ch.assertQueue(COORD_QUEUE, { durable: false, arguments: { "x-message-ttl": 30000 } }, function (err) {
      if (err) {
        try { ch.close(); } catch (e) {}
        callback(null, [WORKER_ID]);
        return;
      }

      var workers = new Set();
      workers.add(WORKER_ID);
      var maxRead = 100;
      var read = 0;

      function done() {
        try { ch.close(); } catch (e) {}
        callback(null, Array.from(workers).sort());
      }

      function readNext() {
        ch.get(COORD_QUEUE, { noAck: false }, function (err, msg) {
          if (err || !msg) {
            done();
            return;
          }
          try {
            var info = JSON.parse(msg.content.toString());
            if (info.workerId) workers.add(info.workerId);
            ch.sendToQueue(COORD_QUEUE, msg.content, { persistent: false });
            ch.ack(msg);
          } catch (e) { ch.ack(msg); }
          read++;
          if (read < maxRead) readNext();
          else done();
        });
      }
      readNext();
    });
  });
}

// --- Discover customer queues (4 parts) and system queues ---
function discoverCustomerQueues(callback) {
  mgmtRequest("/api/queues", function (err, queues) {
    if (err) { callback(err); return; }

    var customerQueues = queues
      .filter(function (q) {
        if (!q.name) return false;
        var parts = q.name.split(".");
        // Customer queues: tenant.webhooks.instanceId.customerId (4 parts)
        // System queues:   tenant.webhooks.instanceId.__system__ (4 parts, ends with __system__)
        return parts.length === 4 && parts[1] === "webhooks";
      })
      .map(function (q) { return q.name; })
      .sort();

    callback(null, customerQueues);
  });
}

// --- Process a webhook message (your business logic goes here) ---
function processWebhook(queueName, webhookData, callback) {
  var parts = queueName.split(".");
  var tenantId = parts[0];
  var instanceId = parts[2];
  var customerId = parts[3];
  var isSystem = customerId === "__system__";

  if (isSystem) {
    console.log("  [SYS] tenant=%s instance=%s event=%s",
      tenantId, instanceId, webhookData.event || "unknown");
  } else {
    console.log("  [MSG] tenant=%s instance=%s customer=%s event=%s seq=%s",
      tenantId, instanceId, customerId, webhookData.event || "unknown",
      webhookData.sequence || "-");
  }

  // ====================================================
  // YOUR AI PROCESSING LOGIC HERE
  // Because prefetch(1), messages for this customer
  // are guaranteed to arrive in order, one at a time.
  // You can safely call your AI, update conversation state, etc.
  // ====================================================

  // Simulate processing time (replace with real logic)
  setTimeout(function () {
    // Log to database AFTER processing (so processed_at reflects when it was done)
    logProcessedMessage(queueName, webhookData, function () {
      callback();
    });
  }, 500);
}

// --- Create consumer for a customer queue ---
function createConsumer(queueName) {
  if (activeConsumers[queueName]) return;
  if (!connection) return;

  // IMPORTANT: Each customer queue gets its OWN channel with prefetch(1)
  // This means:
  // - Messages for customer A are processed one at a time (in order)
  // - But customer A and customer B are processed IN PARALLEL (different channels)
  connection.createChannel(function (err, ch) {
    if (err) { console.error("[!] Channel error:", err.message); return; }

    ch.on("error", function (err) {
      console.error("[!] Consumer channel error on %s: %s", queueName, err.message);
      delete activeConsumers[queueName];
    });

    ch.assertQueue(queueName, { durable: true }, function (err) {
      if (err) { console.error("[!] Assert error:", err.message); return; }

      ch.prefetch(1); // ONE message at a time per customer

      console.log("[*] Consuming: %s", queueName);

      ch.consume(queueName, function (msg) {
        if (!msg) return;

        if (shuttingDown) {
          ch.nack(msg, false, true);
          return;
        }

        inFlightCount++;

        var content = msg.content.toString();
        var webhookData;
        try {
          webhookData = JSON.parse(content);
        } catch (e) {
          console.error("[!] Invalid JSON, discarding");
          ch.ack(msg);
          inFlightCount--;
          return;
        }

        processWebhook(queueName, webhookData, function () {
          ch.ack(msg);
          inFlightCount--;
        });
      }, { noAck: false }, function (err, ok) {
        if (err) { console.error("[!] Consume error:", err.message); return; }
        activeConsumers[queueName] = { channel: ch, consumerTag: ok.consumerTag };
      });
    });
  });
}

function stopConsumer(queueName) {
  if (!activeConsumers[queueName]) return;
  console.log("[*] Stopping: %s", queueName);
  var consumer = activeConsumers[queueName];
  if (consumer.consumerTag && consumer.channel) {
    consumer.channel.cancel(consumer.consumerTag, function () {
      consumer.channel.close(function () {});
    });
  } else {
    consumer.channel.close(function () {});
  }
  delete activeConsumers[queueName];
}

// --- Discovery & balancing ---
function discoverAndBalance() {
  getActiveProcessors(function (err, workers) {
    if (err) { console.error("[!]", err.message); return; }

    var total = workers.length;
    var myIndex = workers.indexOf(WORKER_ID);
    if (myIndex === -1) return;

    console.log("\n[*] Processors: %d, my index: %d", total, myIndex);

    discoverCustomerQueues(function (err, queues) {
      if (err) { console.error("[!]", err.message); return; }

      // Consistent hashing: each customer queue is assigned to exactly ONE worker
      var myQueues = queues.filter(function (q) {
        return (hashToInt(q) % total) === myIndex;
      });

      console.log("[*] Customer queues: %d total, %d mine", queues.length, myQueues.length);

      // Start new consumers
      myQueues.forEach(function (q) {
        if (!activeConsumers[q]) createConsumer(q);
      });

      // Stop consumers that are no longer ours
      Object.keys(activeConsumers).forEach(function (q) {
        if (myQueues.indexOf(q) === -1) stopConsumer(q);
      });

      if (myQueues.length > 0) {
        console.log("[*] My queues: %s", myQueues.join(", "));
      }
    });
  });
}

// --- Bootstrap ---
console.log("[*] Processor Worker: %s", WORKER_ID);

initPostgres(function () {
  amqp.connect(config.RABBITMQ_URL, function (err, conn) {
    if (err) { console.error("[!] RabbitMQ failed:", err.message); process.exit(1); }

    connection = conn;
    console.log("[*] Connected to RabbitMQ");

    discoverAndBalance();
    setInterval(discoverAndBalance, config.DISCOVERY_INTERVAL);
    setInterval(publishHeartbeat, config.HEARTBEAT_INTERVAL);
  });
});

function gracefulShutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log("\n[*] Graceful shutdown started...");
  console.log("[*] In-flight messages: %d", inFlightCount);
  console.log("[*] Step 1: Cancel all consumers (stop receiving new messages)");

  var consumerKeys = Object.keys(activeConsumers);
  var cancelled = 0;

  if (consumerKeys.length === 0) {
    waitForInFlight();
    return;
  }

  consumerKeys.forEach(function (q) {
    var consumer = activeConsumers[q];
    if (consumer && consumer.consumerTag && consumer.channel) {
      consumer.channel.cancel(consumer.consumerTag, function () {
        cancelled++;
        if (cancelled >= consumerKeys.length) waitForInFlight();
      });
    } else {
      cancelled++;
      if (cancelled >= consumerKeys.length) waitForInFlight();
    }
  });

  function waitForInFlight() {
    if (inFlightCount <= 0) {
      closeEverything();
      return;
    }
    console.log("[*] Step 2: Waiting for %d in-flight message(s) to finish...", inFlightCount);
    var checkInterval = setInterval(function () {
      if (inFlightCount <= 0) {
        clearInterval(checkInterval);
        closeEverything();
      } else {
        console.log("[*] Still waiting... %d in-flight", inFlightCount);
      }
    }, 500);

    setTimeout(function () {
      console.error("[!] Force shutdown after 30s timeout. %d messages will be redelivered by RabbitMQ.", inFlightCount);
      clearInterval(checkInterval);
      closeEverything();
    }, 30000);
  }

  function closeEverything() {
    console.log("[*] Step 3: Closing channels and connections...");

    Object.keys(activeConsumers).forEach(function (q) {
      if (activeConsumers[q] && activeConsumers[q].channel) {
        try { activeConsumers[q].channel.close(); } catch (e) {}
      }
    });
    if (coordChannel) try { coordChannel.close(); } catch (e) {}
    if (connection) {
      connection.close(function () {
        console.log("[*] RabbitMQ connection closed");
        if (pgClient) pgClient.end();
        console.log("[*] Shutdown complete.");
        process.exit(0);
      });
    } else {
      if (pgClient) pgClient.end();
      process.exit(0);
    }
  }
}

process.once("SIGINT", gracefulShutdown);
process.once("SIGTERM", gracefulShutdown);
