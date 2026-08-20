var amqp = require("amqplib/callback_api");
var http = require("http");
var { Client } = require("pg");
var crypto = require("crypto");
var os = require("os");
var config = require("./config");

// ============================================================
// PROCESSOR WORKER (LIMITED)
//
// Same as webhook-hierarchy processor, but with:
// 1. MAX_CONSUMERS_PER_WORKER: limits how many customer queues
//    this worker consumes at the same time (default 500).
//    Queues with pending messages are prioritized.
// 2. Customer queues have x-expires TTL — RabbitMQ auto-deletes
//    them after 5 min with no consumers and no messages.
//    The processor must assertQueue with the same x-expires
//    argument, otherwise RabbitMQ rejects the assert.
// ============================================================

var MAX_CONSUMERS = config.MAX_CONSUMERS_PER_WORKER;
var QUEUE_EXPIRES = config.CUSTOMER_QUEUE_EXPIRES_MS;

var WORKER_ID = "processor-" + os.hostname() + "-" + process.pid;
var connection = null;
var pgClient = null;
var activeConsumers = {};
var shuttingDown = false;
var inFlightCount = 0;

// Updated by discoverAndBalance, used by notification handler
var lastKnownWorkerCount = 1;
var lastKnownMyIndex = 0;

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

// --- Discover customer queues WITH message counts ---
function discoverCustomerQueues(callback) {
  mgmtRequest("/api/queues", function (err, queues) {
    if (err) { callback(err); return; }

    var customerQueues = queues
      .filter(function (q) {
        if (!q.name) return false;
        var parts = q.name.split(".");
        return parts.length === 4 && parts[1] === "webhooks";
      })
      .map(function (q) {
        return {
          name: q.name,
          messages: q.messages || 0,       // total messages in queue
          consumers: q.consumers || 0,     // current consumer count
          idleSince: q.idle_since || null,  // when queue became idle
        };
      });

    callback(null, customerQueues);
  });
}

// --- Delete empty idle queues via Management API ---
function cleanupIdleQueues(queues) {
  var now = Date.now();
  var ttl = QUEUE_EXPIRES;

  queues.forEach(function (q) {
    // Only delete if: empty, no consumers, idle for longer than TTL
    if (q.messages > 0 || q.consumers > 0 || !q.idleSince) return;

    var idleMs = now - new Date(q.idleSince).getTime();
    if (idleMs < ttl) return;

    // Delete via Management API
    var vhost = encodeURIComponent("/");
    var qName = encodeURIComponent(q.name);
    var auth = Buffer.from(config.RABBITMQ_USER + ":" + config.RABBITMQ_PASS).toString("base64");

    var req = http.request({
      hostname: config.RABBITMQ_MGMT_HOST,
      port: config.RABBITMQ_MGMT_PORT,
      path: "/api/queues/" + vhost + "/" + qName,
      method: "DELETE",
      headers: { Authorization: "Basic " + auth },
    }, function (res) {
      if (res.statusCode === 204 || res.statusCode === 200) {
        console.log("[CLEANUP] Deleted idle queue: %s (idle for %ds)", q.name, Math.round(idleMs / 1000));
      }
      res.resume();
    });
    req.on("error", function () {});
    req.end();
  });
}

// --- Process a webhook message ---
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

  setTimeout(function () {
    logProcessedMessage(queueName, webhookData, function () {
      callback();
    });
  }, 500);
}

// --- Create consumer for a customer queue ---
function createConsumer(queueName) {
  if (activeConsumers[queueName]) return;
  if (!connection) return;

  connection.createChannel(function (err, ch) {
    if (err) { console.error("[!] Channel error:", err.message); return; }

    ch.on("error", function (err) {
      console.error("[!] Consumer channel error on %s: %s", queueName, err.message);
      delete activeConsumers[queueName];
    });

    ch.assertQueue(queueName, { durable: true }, function (err) {
      if (err) { console.error("[!] Assert error:", err.message); return; }

      ch.prefetch(1);

      console.log("[+] Consuming: %s (active: %d/%d)", queueName,
        Object.keys(activeConsumers).length + 1, MAX_CONSUMERS);

      // Drain timer: after processing a message, if no new message arrives
      // within 2 seconds, the queue is considered "drained" (empty).
      // If we're at the consumer limit, stop this consumer to free the slot
      // and immediately try to pick up a queue that has pending messages.
      var drainTimer = null;

      function startDrainTimer() {
        clearDrainTimer();
        drainTimer = setTimeout(function () {
          drainTimer = null;
          // Queue has been idle for 2s after last ACK — likely empty
          var currentCount = Object.keys(activeConsumers).length;
          if (currentCount >= MAX_CONSUMERS) {
            // At limit — release this slot so a queue with messages can take it
            console.log("[DRAIN] %s is empty, freeing slot (%d/%d)", queueName, currentCount - 1, MAX_CONSUMERS);
            stopConsumer(queueName, true);
            // Immediately try to fill the freed slot
            fillFreeSlots();
          }
        }, 2000);
      }

      function clearDrainTimer() {
        if (drainTimer) { clearTimeout(drainTimer); drainTimer = null; }
      }

      ch.consume(queueName, function (msg) {
        if (!msg) return;

        // New message arrived — queue is NOT drained
        clearDrainTimer();

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
          startDrainTimer();
          return;
        }

        // Mark this queue as having an in-flight message
        if (activeConsumers[queueName]) activeConsumers[queueName].busy = true;

        processWebhook(queueName, webhookData, function () {
          try {
            ch.ack(msg);
          } catch (e) {
            console.error("[!] ACK failed (channel closed) for %s, will be redelivered", queueName);
          }
          inFlightCount--;
          if (activeConsumers[queueName]) activeConsumers[queueName].busy = false;

          // After ACK, start drain timer.
          // If another message arrives before 2s, the timer is cancelled.
          // If no message arrives, the queue is drained and slot is freed.
          startDrainTimer();
        });
      }, { noAck: false }, function (err, ok) {
        if (err) { console.error("[!] Consume error:", err.message); return; }
        activeConsumers[queueName] = { channel: ch, consumerTag: ok.consumerTag, busy: false, clearDrain: clearDrainTimer };
      });
    });
  });
}

function stopConsumer(queueName, force) {
  if (!activeConsumers[queueName]) return;

  var consumer = activeConsumers[queueName];

  // Don't close channel while a message is being processed — the ACK would crash.
  // Cancel the consumer (stops NEW deliveries) but keep channel open for the in-flight ACK.
  // On next discovery cycle, busy will be false and we can fully close.
  if (consumer.busy && !force) {
    if (consumer.consumerTag && consumer.channel) {
      console.log("[-] Cancelling (busy, channel stays open): %s", queueName);
      consumer.channel.cancel(consumer.consumerTag, function () {});
      consumer.consumerTag = null; // mark as cancelled so we don't cancel again
    }
    return; // don't delete from activeConsumers yet — will clean up next cycle
  }

  console.log("[-] Stopping: %s (active: %d/%d)", queueName,
    Object.keys(activeConsumers).length - 1, MAX_CONSUMERS);

  // Clear drain timer if exists
  if (consumer.clearDrain) consumer.clearDrain();

  if (consumer.consumerTag && consumer.channel) {
    consumer.channel.cancel(consumer.consumerTag, function () {
      try { consumer.channel.close(); } catch (e) {}
    });
  } else if (consumer.channel) {
    try { consumer.channel.close(); } catch (e) {}
  }
  delete activeConsumers[queueName];
}

// --- Fill free slots with queues that have pending messages ---
// Called after a consumer is drained (queue empty) and stopped.
// Lightweight: queries the Management API for queues with messages,
// filters to ours (consistent hashing), and starts consumers.
var fillInProgress = false;
function fillFreeSlots() {
  if (fillInProgress) return;
  fillInProgress = true;

  var availableSlots = MAX_CONSUMERS - Object.keys(activeConsumers).length;
  if (availableSlots <= 0) { fillInProgress = false; return; }

  discoverCustomerQueues(function (err, queues) {
    fillInProgress = false;
    if (err) return;

    // Filter to my queues with messages, not already consuming
    var candidates = queues.filter(function (q) {
      if (activeConsumers[q.name]) return false;
      if (q.messages === 0) return false;
      return (hashToInt(q.name) % lastKnownWorkerCount) === lastKnownMyIndex;
    });

    // Sort by message count desc
    candidates.sort(function (a, b) { return b.messages - a.messages; });

    var toStart = Math.min(candidates.length, availableSlots);
    for (var i = 0; i < toStart; i++) {
      console.log("[FILL] Starting consumer for %s (%d msgs pending)", candidates[i].name, candidates[i].messages);
      createConsumer(candidates[i].name);
    }

    if (toStart > 0) {
      console.log("[FILL] Started %d new consumers, %d still waiting",
        toStart, Math.max(0, candidates.length - toStart));
    }
  });
}

// --- Discovery & balancing with consumer limit ---
function discoverAndBalance() {
  getActiveProcessors(function (err, workers) {
    if (err) { console.error("[!]", err.message); return; }

    var total = workers.length;
    var myIndex = workers.indexOf(WORKER_ID);
    if (myIndex === -1) return;

    // Store for notification handler
    lastKnownWorkerCount = total;
    lastKnownMyIndex = myIndex;

    discoverCustomerQueues(function (err, queues) {
      if (err) { console.error("[!]", err.message); return; }

      // Filter: only queues assigned to this worker by consistent hashing
      var myQueues = queues.filter(function (q) {
        return (hashToInt(q.name) % total) === myIndex;
      });

      // Step 0: Clean up consumers that were cancelled but couldn't close (were busy)
      Object.keys(activeConsumers).forEach(function (qName) {
        var c = activeConsumers[qName];
        if (!c.consumerTag && !c.busy) {
          // Was cancelled last cycle, now idle — close channel and remove
          console.log("[-] Cleaning up cancelled consumer: %s", qName);
          try { c.channel.close(); } catch (e) {}
          delete activeConsumers[qName];
        }
      });

      // Step 1: Stop consumers for queues no longer assigned to us
      Object.keys(activeConsumers).forEach(function (qName) {
        var stillMine = myQueues.some(function (q) { return q.name === qName; });
        if (!stillMine) stopConsumer(qName);
      });

      // Step 2: Sort by priority — queues with messages first, then by message count desc
      myQueues.sort(function (a, b) {
        // Queues with messages come first
        if (a.messages > 0 && b.messages === 0) return -1;
        if (a.messages === 0 && b.messages > 0) return 1;
        // Among queues with messages, most messages first
        return b.messages - a.messages;
      });

      var currentCount = Object.keys(activeConsumers).length;
      var availableSlots = MAX_CONSUMERS - currentCount;

      // Step 3: If over limit, stop consumers for EMPTY queues to make room
      if (availableSlots <= 0 && myQueues.some(function (q) { return q.messages > 0 && !activeConsumers[q.name]; })) {
        // Find active consumers for queues that are now empty
        var emptyActiveQueues = Object.keys(activeConsumers).filter(function (qName) {
          var qInfo = myQueues.find(function (q) { return q.name === qName; });
          return qInfo && qInfo.messages === 0;
        });

        // Stop empty queues to free slots for queues with messages
        var queuesToFree = emptyActiveQueues.slice(0, Math.min(emptyActiveQueues.length, 50));
        queuesToFree.forEach(function (qName) {
          stopConsumer(qName);
        });

        currentCount = Object.keys(activeConsumers).length;
        availableSlots = MAX_CONSUMERS - currentCount;
      }

      // Step 4: Start consumers for new queues, respecting the limit
      var started = 0;
      for (var i = 0; i < myQueues.length; i++) {
        var q = myQueues[i];
        if (activeConsumers[q.name]) continue; // already consuming
        if (started >= availableSlots) break;   // hit the limit

        createConsumer(q.name);
        started++;
      }

      var waitingQueues = myQueues.length - Object.keys(activeConsumers).length - started;

      console.log("\n[*] Processors: %d | My queues: %d | Active: %d/%d | Waiting: %d",
        total, myQueues.length,
        Object.keys(activeConsumers).length + started, MAX_CONSUMERS,
        Math.max(0, waitingQueues));

      // Step 5: Cleanup empty idle queues (replaces x-expires)
      // Only worker 0 does cleanup to avoid race conditions
      if (myIndex === 0) {
        cleanupIdleQueues(queues);
      }
    });
  });
}

// --- Listen for new queue notifications from router ---
// Uses a FANOUT exchange: every worker receives every notification.
// Each worker creates its own EXCLUSIVE queue bound to the exchange.
// Exclusive = auto-deleted when this worker disconnects.
// This way:
//   - ALL workers see every "new queue" notification
//   - Each worker checks consistent hashing → only the owner starts consuming
//   - No risk of the wrong worker getting the notification and discarding it
function startNotificationListener() {
  if (!connection) return;

  connection.createChannel(function (err, ch) {
    if (err) { console.error("[!] Notify channel error:", err.message); return; }

    ch.on("error", function (err) {
      console.error("[!] Notify channel error:", err.message);
    });

    // Step 1: Assert the fanout exchange (same name the router uses)
    ch.assertExchange(config.NOTIFY_EXCHANGE, "fanout", { durable: false }, function (err) {
      if (err) { console.error("[!] Notify exchange error:", err.message); return; }

      // Step 2: Create an exclusive queue for THIS worker only.
      // Empty name = RabbitMQ generates a unique name (amq.gen-xxx).
      // exclusive: true = only this connection can use it, auto-deleted on disconnect.
      ch.assertQueue("", { exclusive: true }, function (err, ok) {
        if (err) { console.error("[!] Notify queue error:", err.message); return; }

        var myNotifyQueue = ok.queue;

        // Step 3: Bind our exclusive queue to the fanout exchange.
        // Now every message published to the exchange is copied to our queue.
        ch.bindQueue(myNotifyQueue, config.NOTIFY_EXCHANGE, "", {}, function (err) {
          if (err) { console.error("[!] Notify bind error:", err.message); return; }

          // Step 4: Consume notifications
          ch.consume(myNotifyQueue, function (msg) {
            if (!msg) return;

            var queueName = msg.content.toString();
            ch.ack(msg);

            if (shuttingDown) return;

            // Check if this queue belongs to us (consistent hashing)
            var isMyQueue = (hashToInt(queueName) % lastKnownWorkerCount) === lastKnownMyIndex;
            if (!isMyQueue) return;

            // Already consuming?
            if (activeConsumers[queueName]) return;

            // Check limit
            var currentCount = Object.keys(activeConsumers).length;
            if (currentCount >= MAX_CONSUMERS) {
              console.log("[NOTIFY] %s is mine but at limit (%d/%d), will pick up on next cycle",
                queueName, currentCount, MAX_CONSUMERS);
              return;
            }

            console.log("[NOTIFY] New queue %s → starting consumer immediately", queueName);
            createConsumer(queueName);
          }, { noAck: false });

          console.log("[*] Listening for notifications on exchange: %s (my queue: %s)", config.NOTIFY_EXCHANGE, myNotifyQueue);
        });
      });
    });
  });
}

// --- Bootstrap ---
console.log("[*] Processor Worker: %s", WORKER_ID);
console.log("[*] Max consumers per worker: %d", MAX_CONSUMERS);
console.log("[*] Queue expires after: %d ms of inactivity", QUEUE_EXPIRES);

initPostgres(function () {
  amqp.connect(config.RABBITMQ_URL, function (err, conn) {
    if (err) { console.error("[!] RabbitMQ failed:", err.message); process.exit(1); }

    connection = conn;
    console.log("[*] Connected to RabbitMQ");

    // Start notification listener FIRST (instant reaction to new queues)
    startNotificationListener();

    // Discovery is now a fallback: rebalancing, cleanup, catch missed notifications
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
