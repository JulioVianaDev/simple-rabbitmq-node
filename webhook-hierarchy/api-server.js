var http = require("http");
var amqp = require("amqplib/callback_api");
var config = require("./config");

// ============================================================
// API SERVER
// Receives webhooks on POST /webhook and routes to instance queue
//
// ZERO dependency on PostgreSQL.
// The queue name is just: webhooks.raw.{instanceId}
// The router-worker will resolve the tenant later.
// This way, if the DB is down, webhooks are still safely queued.
// ============================================================

var rabbitConnection = null;
var rabbitChannel = null;

function initRabbit(callback) {
  amqp.connect(config.RABBITMQ_URL, function (err, conn) {
    if (err) {
      console.error("[!] RabbitMQ connection failed:", err.message);
      setTimeout(function () { initRabbit(callback); }, 3000);
      return;
    }
    rabbitConnection = conn;
    conn.createChannel(function (err, ch) {
      if (err) {
        console.error("[!] Channel creation failed:", err.message);
        return;
      }
      rabbitChannel = ch;
      console.log("[*] Connected to RabbitMQ");
      if (callback) callback();
    });
  });
}

// Publish to a raw instance queue (no tenant resolution needed)
var assertedQueues = {};
function publishToRawQueue(instanceId, webhookData) {
  var queueName = "webhooks.raw." + instanceId;

  function doPublish() {
    var msg = JSON.stringify(webhookData);
    rabbitChannel.sendToQueue(queueName, Buffer.from(msg), { persistent: true });
    console.log("[->] %s : %s", queueName, msg.substring(0, 80));
  }

  if (assertedQueues[queueName]) {
    doPublish();
    return;
  }

  rabbitChannel.assertQueue(queueName, { durable: true }, function (err) {
    if (err) {
      console.error("[!] Failed to assert queue %s:", queueName, err.message);
      return;
    }
    assertedQueues[queueName] = true;
    doPublish();
  });
}

// HTTP Server
function startServer() {
  var server = http.createServer(function (req, res) {
    if (req.method === "POST" && req.url === "/webhook") {
      var body = "";
      req.on("data", function (chunk) { body += chunk; });
      req.on("end", function () {
        try {
          var data = JSON.parse(body);
        } catch (e) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "Invalid JSON" }));
          return;
        }

        var instanceId = data.instanceId;
        if (!instanceId) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "Missing instanceId" }));
          return;
        }

        data.receivedAt = new Date().toISOString();

        publishToRawQueue(instanceId, data);

        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true, queue: "webhooks.raw." + instanceId }));
      });
    } else {
      res.writeHead(404);
      res.end("Not found");
    }
  });

  server.listen(3001, function () {
    console.log("\n[*] API Server listening on http://localhost:3001");
    console.log("[*] POST /webhook with { instanceId, event, ... }");
    console.log("[*] No database dependency - webhooks are safe even if DB is down");
  });
}

// Bootstrap - only needs RabbitMQ, no PostgreSQL
initRabbit(function () {
  startServer();
});
