var fs = require("fs");
var { Client } = require("pg");
var config = require("./config");

// ============================================================
// INTEGRITY VERIFIER
//
// Reads the manifest from test-integrity.js and compares against
// what was actually saved in the database.
//
// Checks:
//   1. Total count matches
//   2. No missing messages (sent but not in DB)
//   3. No duplicate messages (same messageId twice in DB)
//   4. No extra messages (in DB but not in manifest)
//   5. Worker distribution (how many messages each worker processed)
//
// Usage:
//   node verify-integrity.js test-manifest-XXXXXXXX.json
// ============================================================

var manifestFile = process.argv[2];
if (!manifestFile) {
  // Try to find the most recent manifest
  var files = fs.readdirSync(".").filter(function (f) {
    return f.startsWith("test-manifest-") && f.endsWith(".json");
  }).sort();

  if (files.length === 0) {
    console.error("[!] No manifest file found. Run test-integrity.js first.");
    process.exit(1);
  }
  manifestFile = files[files.length - 1];
  console.log("[*] Using most recent manifest: %s\n", manifestFile);
}

var manifest = JSON.parse(fs.readFileSync(manifestFile, "utf-8"));
var expectedIds = new Set(manifest.messageIds);

console.log("=".repeat(60));
console.log("  INTEGRITY VERIFICATION");
console.log("=".repeat(60));
console.log("  Test ID:    %s", manifest.testId);
console.log("  Customers:  %d", manifest.totalCustomers);
console.log("  Created at: %s", manifest.createdAt);
console.log("  Manifest:   %s", manifestFile);
console.log("=".repeat(60));

var pgClient = new Client(config.PG_CONFIG);
pgClient.connect(function (err) {
  if (err) {
    console.error("[!] PG connection failed:", err.message);
    process.exit(1);
  }

  // Query all messages with matching testId (stored in body field)
  var query = "SELECT id, customer_id, body, worker_id, processed_at FROM processed_messages WHERE body LIKE $1 ORDER BY id";
  pgClient.query(query, [manifest.testId + "-%"], function (err, result) {
    if (err) {
      console.error("[!] Query failed:", err.message);
      pgClient.end();
      process.exit(1);
    }

    var rows = result.rows;
    var dbIds = rows.map(function (r) { return r.body; });
    var dbIdSet = new Set(dbIds);

    console.log("\n--- 1. COUNT CHECK ---");
    console.log("  Expected: %d", expectedIds.size);
    console.log("  Found:    %d", rows.length);
    if (rows.length === expectedIds.size) {
      console.log("  Result:   PASS");
    } else {
      console.log("  Result:   FAIL (difference: %d)", rows.length - expectedIds.size);
    }

    // Check duplicates
    console.log("\n--- 2. DUPLICATE CHECK ---");
    var seen = {};
    var duplicates = [];
    rows.forEach(function (r) {
      if (seen[r.body]) {
        duplicates.push({
          messageId: r.body,
          firstId: seen[r.body].id,
          firstWorker: seen[r.body].worker_id,
          duplicateId: r.id,
          duplicateWorker: r.worker_id,
        });
      } else {
        seen[r.body] = r;
      }
    });

    if (duplicates.length === 0) {
      console.log("  Duplicates: 0");
      console.log("  Result:     PASS");
    } else {
      console.log("  Duplicates: %d", duplicates.length);
      console.log("  Result:     FAIL");
      duplicates.slice(0, 10).forEach(function (d) {
        console.log("    messageId=%s: row %d (%s) vs row %d (%s)",
          d.messageId, d.firstId, d.firstWorker, d.duplicateId, d.duplicateWorker);
      });
      if (duplicates.length > 10) {
        console.log("    ... and %d more", duplicates.length - 10);
      }
    }

    // Check missing
    console.log("\n--- 3. MISSING CHECK ---");
    var missing = [];
    expectedIds.forEach(function (msgId) {
      if (!dbIdSet.has(msgId)) {
        missing.push(msgId);
      }
    });

    if (missing.length === 0) {
      console.log("  Missing:  0");
      console.log("  Result:   PASS");
    } else {
      console.log("  Missing:  %d", missing.length);
      console.log("  Result:   FAIL");
      missing.slice(0, 10).forEach(function (m) {
        console.log("    %s", m);
      });
      if (missing.length > 10) {
        console.log("    ... and %d more", missing.length - 10);
      }
    }

    // Check extras (in DB but not in manifest)
    console.log("\n--- 4. EXTRA CHECK ---");
    var extras = [];
    dbIds.forEach(function (msgId) {
      if (!expectedIds.has(msgId)) {
        extras.push(msgId);
      }
    });

    if (extras.length === 0) {
      console.log("  Extras:   0");
      console.log("  Result:   PASS");
    } else {
      console.log("  Extras:   %d", extras.length);
      console.log("  Result:   FAIL");
      extras.slice(0, 10).forEach(function (e) {
        console.log("    %s", e);
      });
    }

    // Worker distribution
    console.log("\n--- 5. WORKER DISTRIBUTION ---");
    var workerCounts = {};
    rows.forEach(function (r) {
      if (!workerCounts[r.worker_id]) workerCounts[r.worker_id] = 0;
      workerCounts[r.worker_id]++;
    });

    var workerIds = Object.keys(workerCounts).sort();
    workerIds.forEach(function (w) {
      console.log("  %s: %d messages", w, workerCounts[w]);
    });
    console.log("  Total workers: %d", workerIds.length);

    // Check pending in RabbitMQ
    console.log("\n--- 6. PENDING IN QUEUES ---");
    checkPendingQueues(function (pending) {
      if (pending === 0) {
        console.log("  Pending:  0");
        console.log("  Result:   PASS");
      } else if (pending === -1) {
        console.log("  Pending:  (could not check — Management API unavailable)");
        console.log("  Result:   SKIP");
      } else {
        console.log("  Pending:  %d messages still in customer queues", pending);
        console.log("  Result:   WAIT (messages are still being processed)");
      }

      // Final summary
      var allPass = (
        rows.length === expectedIds.size &&
        duplicates.length === 0 &&
        missing.length === 0 &&
        extras.length === 0
      );

      console.log("\n" + "=".repeat(60));
      if (allPass && pending === 0) {
        console.log("  FINAL RESULT: ALL CHECKS PASSED");
      } else if (allPass && pending > 0) {
        console.log("  FINAL RESULT: WAIT — %d messages still processing", pending);
        console.log("  Run this script again after processing completes.");
      } else {
        console.log("  FINAL RESULT: SOME CHECKS FAILED");
        if (missing.length > 0 && pending > 0) {
          console.log("  Note: %d missing, %d pending — may just need more time.", missing.length, pending);
        }
      }
      console.log("=".repeat(60));

      pgClient.end();
      process.exit(allPass && pending === 0 ? 0 : 1);
    });
  });
});

function checkPendingQueues(callback) {
  var auth = Buffer.from(config.RABBITMQ_USER + ":" + config.RABBITMQ_PASS).toString("base64");
  var req = http.request({
    hostname: config.RABBITMQ_MGMT_HOST,
    port: config.RABBITMQ_MGMT_PORT,
    path: "/api/queues",
    method: "GET",
    headers: { Authorization: "Basic " + auth },
  }, function (res) {
    var data = "";
    res.on("data", function (chunk) { data += chunk; });
    res.on("end", function () {
      if (res.statusCode !== 200) { callback(-1); return; }
      try {
        var queues = JSON.parse(data);
        var pending = 0;
        queues.forEach(function (q) {
          if (!q.name) return;
          var parts = q.name.split(".");
          // Customer queues: 4 parts, second is "webhooks"
          if (parts.length === 4 && parts[1] === "webhooks") {
            pending += (q.messages || 0);
          }
          // Raw queues
          if (parts.length === 3 && parts[0] === "webhooks" && parts[1] === "raw") {
            pending += (q.messages || 0);
          }
        });
        callback(pending);
      } catch (e) { callback(-1); }
    });
  });
  req.on("error", function () { callback(-1); });
  req.end();
}

var http = require("http");
