var amqp = require("amqplib/callback_api");
var http = require("http");
var { Client } = require("pg");
var crypto = require("crypto");
var os = require("os");
var config = require("./config");

// ============================================================
// ROUTER WORKER
// Consumes from raw instance queues: webhooks.raw.{instanceId}
// Resolves tenantId + customerId from DB, routes to customer queue
// Events without customerId go to __system__ queue
//
// If DB is down: message stays in the raw queue (NACK + requeue)
// Nothing is lost. The message will be retried when DB comes back.
//
// Multiple router workers can run - they split raw queues
// via consistent hashing
// ============================================================

var WORKER_ID = "router-" + os.hostname() + "-" + process.pid;
var connection = null;
var pgClient = null;
var pgConnected = false;
var activeConsumers = {};
var publishChannel = null;
var assertedQueues = {};
var shuttingDown = false;
var inFlightCount = 0; // how many messages are currently being processed

// Cache: instanceId -> tenantId
var tenantCache = {};

// --- PostgreSQL ---
function initPostgres(callback) {
  pgClient = new Client(config.PG_CONFIG);
  pgClient.connect(function (err) {
    if (err) {
      console.error("[!] PG connection failed:", err.message);
      pgConnected = false;
      setTimeout(function () { initPostgres(callback); }, 3000);
      return;
    }
    pgConnected = true;
    console.log("[*] Connected to PostgreSQL");

    // Pre-load tenant cache
    pgClient.query("SELECT instance_id, tenant_id FROM instances", function (err, result) {
      if (!err && result) {
        result.rows.forEach(function (row) {
          tenantCache[row.instance_id] = row.tenant_id;
        });
        console.log("[*] Loaded %d instance->tenant mappings", result.rows.length);
      }
      if (callback) callback();
    });
  });

  pgClient.on("error", function (err) {
    console.error("[!] PG error:", err.message);
    pgConnected = false;
    setTimeout(function () { initPostgres(); }, 3000);
  });
}

// --- Resolve tenantId ---
function resolveTenant(instanceId, callback) {
  if (tenantCache[instanceId]) {
    callback(null, tenantCache[instanceId]);
    return;
  }

  if (!pgConnected) {
    callback(new Error("DB unavailable"));
    return;
  }

  pgClient.query(
    "SELECT tenant_id FROM instances WHERE instance_id = $1",
    [instanceId],
    function (err, result) {
      if (err) { callback(err); return; }
      if (result.rows.length === 0) {
        callback(new Error("Unknown instance: " + instanceId));
        return;
      }
      tenantCache[instanceId] = result.rows[0].tenant_id;
      callback(null, result.rows[0].tenant_id);
    }
  );
}

// --- Identify customerId ---
function identifyCustomer(webhookData, tenantId, callback) {
  var systemEvents = ["instance.connected", "instance.disconnected", "instance.qrcode", "instance.status"];
  if (systemEvents.indexOf(webhookData.event) !== -1) {
    callback(null, null);
    return;
  }

  var phone = webhookData.remoteJid || webhookData.phone || webhookData.from;
  if (!phone) {
    callback(null, null);
    return;
  }

  phone = phone.replace(/@.*/, "").replace(/[^0-9]/g, "");

  if (!pgConnected) {
    callback(new Error("DB unavailable"));
    return;
  }

  pgClient.query(
    "SELECT id FROM contacts WHERE phone = $1 AND instance_id = $2",
    [phone, webhookData.instanceId],
    function (err, result) {
      if (err) { callback(err); return; }

      if (result.rows.length > 0) {
        callback(null, result.rows[0].id);
      } else {
        var newId = "auto_" + phone;
        pgClient.query(
          "INSERT INTO contacts (id, tenant_id, instance_id, phone, name) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
          [newId, tenantId, webhookData.instanceId, phone, "Auto " + phone],
          function (err) {
            if (err) console.error("[!] Failed to auto-create contact:", err.message);
            callback(null, newId);
          }
        );
      }
    }
  );
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
var COORD_QUEUE = "router-coordination";
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

function getActiveRouters(callback) {
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

// --- Discover raw instance queues (pattern: webhooks.raw.{instanceId}) ---
function discoverRawQueues(callback) {
  mgmtRequest("/api/queues", function (err, queues) {
    if (err) { callback(err); return; }

    var rawQueues = queues
      .filter(function (q) {
        if (!q.name) return false;
        var parts = q.name.split(".");
        // Raw queues: webhooks.raw.{instanceId} (3 parts)
        return parts.length === 3 && parts[0] === "webhooks" && parts[1] === "raw";
      })
      .map(function (q) { return q.name; })
      .sort();

    callback(null, rawQueues);
  });
}

// --- Publish to customer queue ---
function publishToCustomerQueue(tenantId, instanceId, customerId, data, callback) {
  var queueName;
  if (customerId) {
    queueName = tenantId + ".webhooks." + instanceId + "." + customerId;
  } else {
    queueName = tenantId + ".webhooks." + instanceId + ".__system__";
  }

  function doPublish() {
    var msg = JSON.stringify(data);
    publishChannel.sendToQueue(queueName, Buffer.from(msg), { persistent: true });
    console.log("  [->] %s", queueName);
    if (callback) callback(null);
  }

  if (assertedQueues[queueName]) {
    doPublish();
    return;
  }

  publishChannel.assertQueue(queueName, { durable: true }, function (err) {
    if (err) {
      console.error("[!] Failed to assert queue:", queueName, err.message);
      if (callback) callback(err);
      return;
    }
    assertedQueues[queueName] = true;
    doPublish();
  });
}

// --- Create consumer for a raw instance queue ---
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

      console.log("[*] Consuming: %s", queueName);

      ch.consume(queueName, function (msg) {
        if (!msg) return;

        // If shutting down, reject immediately so another worker picks it up
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
          console.error("[!] Invalid JSON in %s, discarding", queueName);
          ch.ack(msg);
          inFlightCount--;
          return;
        }

        var instanceId = webhookData.instanceId;

        console.log("[<-] %s | event=%s", queueName, webhookData.event || "unknown");

        // Step 1: Resolve tenant
        resolveTenant(instanceId, function (err, tenantId) {
          if (err) {
            console.error("[!] Cannot resolve tenant for %s: %s → NACK requeue", instanceId, err.message);
            ch.nack(msg, false, true);
            inFlightCount--;
            return;
          }

          // Step 2: Identify customer
          identifyCustomer(webhookData, tenantId, function (err, customerId) {
            if (err) {
              console.error("[!] Cannot identify customer: %s → NACK requeue", err.message);
              ch.nack(msg, false, true);
              inFlightCount--;
              return;
            }

            // Step 3: Enrich and publish to customer queue
            webhookData.tenantId = tenantId;
            webhookData.customerId = customerId;
            publishToCustomerQueue(tenantId, instanceId, customerId, webhookData, function (err) {
              if (err) {
                // Publish failed — NACK so we retry
                ch.nack(msg, false, true);
                inFlightCount--;
                return;
              }
              // ACK only AFTER publish is confirmed
              ch.ack(msg);
              inFlightCount--;
            });
          });
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
  console.log("[*] Stopping consumer: %s", queueName);
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
  getActiveRouters(function (err, workers) {
    if (err) { console.error("[!]", err.message); return; }

    var total = workers.length;
    var myIndex = workers.indexOf(WORKER_ID);
    if (myIndex === -1) return;

    console.log("[*] Routers: %d, my index: %d", total, myIndex);

    discoverRawQueues(function (err, queues) {
      if (err) { console.error("[!]", err.message); return; }

      var myQueues = queues.filter(function (q) {
        return (hashToInt(q) % total) === myIndex;
      });

      console.log("[*] Raw queues: %d total, %d mine", queues.length, myQueues.length);

      myQueues.forEach(function (q) {
        if (!activeConsumers[q]) createConsumer(q);
      });

      Object.keys(activeConsumers).forEach(function (q) {
        if (myQueues.indexOf(q) === -1) stopConsumer(q);
      });
    });
  });
}

// --- Bootstrap ---
console.log("[*] Router Worker: %s", WORKER_ID);

initPostgres(function () {
  amqp.connect(config.RABBITMQ_URL, function (err, conn) {
    if (err) { console.error("[!] RabbitMQ failed:", err.message); process.exit(1); }

    connection = conn;
    console.log("[*] Connected to RabbitMQ");

    conn.createChannel(function (err, ch) {
      if (err) { console.error("[!] Publish channel error:", err.message); process.exit(1); }
      publishChannel = ch;
      ch.on("error", function (err) {
        console.error("[!] Publish channel error:", err.message);
      });
      console.log("[*] Publish channel ready");

      discoverAndBalance();
      setInterval(discoverAndBalance, config.DISCOVERY_INTERVAL);
      setInterval(publishHeartbeat, config.HEARTBEAT_INTERVAL);
    });
  });
});

function gracefulShutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log("\n[*] Graceful shutdown started...");
  console.log("[*] In-flight messages: %d", inFlightCount);
  console.log("[*] Step 1: Cancel all consumers (stop receiving new messages)");

  // Step 1: Cancel consumers on each channel (stops deliveries, but channel stays open
  // so we can still ACK/NACK in-flight messages)
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

  // Step 2: Wait for in-flight messages to finish processing
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

    // Force kill after 30 seconds if messages are stuck
    setTimeout(function () {
      console.error("[!] Force shutdown after 30s timeout. %d messages will be redelivered by RabbitMQ.", inFlightCount);
      clearInterval(checkInterval);
      closeEverything();
    }, 30000);
  }

  // Step 3: Close channels, connection, DB
  function closeEverything() {
    console.log("[*] Step 3: Closing channels and connections...");

    Object.keys(activeConsumers).forEach(function (q) {
      if (activeConsumers[q] && activeConsumers[q].channel) {
        try { activeConsumers[q].channel.close(); } catch (e) {}
      }
    });
    if (publishChannel) try { publishChannel.close(); } catch (e) {}
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
