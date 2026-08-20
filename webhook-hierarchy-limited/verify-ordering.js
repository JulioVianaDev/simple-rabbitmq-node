var fs = require("fs");
var http = require("http");
var { Client } = require("pg");
var config = require("./config");

// ============================================================
// ORDERING + INTEGRITY VERIFIER
//
// Checks:
//   1. Count: all messages saved (no loss)
//   2. Duplicates: no message processed twice
//   3. Missing: no message lost
//   4. ORDER: for each customer, sequence was processed 1 → 2 → 3
//   5. Worker distribution
//   6. Pending in queues
//
// Usage:
//   node verify-ordering.js
//   node verify-ordering.js test-manifest-XXXXXXXX.json
// ============================================================

var manifestFile = process.argv[2];
if (!manifestFile) {
  var files = fs.readdirSync(".").filter(function (f) {
    return f.startsWith("test-manifest-") && f.endsWith(".json");
  }).sort();

  if (files.length === 0) {
    console.error("[!] No manifest file found. Run test-ordering.js first.");
    process.exit(1);
  }
  manifestFile = files[files.length - 1];
  console.log("[*] Using most recent manifest: %s\n", manifestFile);
}

var manifest = JSON.parse(fs.readFileSync(manifestFile, "utf-8"));
var expectedIds = new Set(manifest.messageIds);

console.log("=".repeat(60));
console.log("  ORDERING + INTEGRITY VERIFICATION");
console.log("=".repeat(60));
console.log("  Test ID:       %s", manifest.testId);
console.log("  Customers:     %d", manifest.totalCustomers);
console.log("  Msgs/customer: %d", manifest.msgsPerCustomer || 1);
console.log("  Total msgs:    %d", manifest.totalMessages || manifest.messageIds.length);
console.log("  Created at:    %s", manifest.createdAt);
console.log("=".repeat(60));

var pgClient = new Client(config.PG_CONFIG);
pgClient.connect(function (err) {
  if (err) {
    console.error("[!] PG connection failed:", err.message);
    process.exit(1);
  }

  var query = "SELECT id, customer_id, instance_id, body, sequence, worker_id, processed_at " +
    "FROM processed_messages WHERE body LIKE $1 ORDER BY customer_id, instance_id, processed_at";

  pgClient.query(query, [manifest.testId + "-%"], function (err, result) {
    if (err) {
      console.error("[!] Query failed:", err.message);
      pgClient.end();
      process.exit(1);
    }

    var rows = result.rows;
    var dbIds = rows.map(function (r) { return r.body; });
    var dbIdSet = new Set(dbIds);

    // --- 1. COUNT ---
    console.log("\n--- 1. COUNT CHECK ---");
    console.log("  Expected: %d", expectedIds.size);
    console.log("  Found:    %d", rows.length);
    var countPass = rows.length === expectedIds.size;
    console.log("  Result:   %s", countPass ? "PASS" : "FAIL (diff: " + (rows.length - expectedIds.size) + ")");

    // --- 2. DUPLICATES ---
    console.log("\n--- 2. DUPLICATE CHECK ---");
    var seen = {};
    var duplicates = [];
    rows.forEach(function (r) {
      if (seen[r.body]) {
        duplicates.push({
          messageId: r.body,
          firstId: seen[r.body].id,
          firstWorker: seen[r.body].worker_id,
          dupeId: r.id,
          dupeWorker: r.worker_id,
        });
      } else {
        seen[r.body] = r;
      }
    });
    var dupePass = duplicates.length === 0;
    console.log("  Duplicates: %d", duplicates.length);
    console.log("  Result:     %s", dupePass ? "PASS" : "FAIL");
    duplicates.slice(0, 5).forEach(function (d) {
      console.log("    %s: row %d (%s) vs row %d (%s)",
        d.messageId, d.firstId, d.firstWorker, d.dupeId, d.dupeWorker);
    });

    // --- 3. MISSING ---
    console.log("\n--- 3. MISSING CHECK ---");
    var missing = [];
    expectedIds.forEach(function (msgId) {
      if (!dbIdSet.has(msgId)) missing.push(msgId);
    });
    var missingPass = missing.length === 0;
    console.log("  Missing:  %d", missing.length);
    console.log("  Result:   %s", missingPass ? "PASS" : "FAIL");
    missing.slice(0, 10).forEach(function (m) {
      console.log("    %s", m);
    });
    if (missing.length > 10) console.log("    ... and %d more", missing.length - 10);

    // --- 4. ORDERING PER CUSTOMER ---
    console.log("\n--- 4. ORDERING CHECK ---");
    // Group by customer, check that sequence is always increasing in processed_at order
    var byCustomer = {};
    rows.forEach(function (r) {
      var key = r.customer_id + "|" + r.instance_id;
      if (!byCustomer[key]) byCustomer[key] = [];
      byCustomer[key].push(r);
    });

    var orderErrors = [];
    var customersChecked = 0;
    Object.keys(byCustomer).forEach(function (key) {
      var msgs = byCustomer[key];
      // Already sorted by processed_at (ORDER BY in query)
      customersChecked++;
      for (var i = 1; i < msgs.length; i++) {
        var prev = msgs[i - 1];
        var curr = msgs[i];
        if (curr.sequence <= prev.sequence) {
          orderErrors.push({
            customer: key,
            position: i,
            prevSeq: prev.sequence,
            prevBody: prev.body,
            prevAt: prev.processed_at,
            currSeq: curr.sequence,
            currBody: curr.body,
            currAt: curr.processed_at,
          });
        }
      }
    });

    var orderPass = orderErrors.length === 0;
    console.log("  Customers checked: %d", customersChecked);
    console.log("  Out-of-order:      %d", orderErrors.length);
    console.log("  Result:            %s", orderPass ? "PASS" : "FAIL");
    orderErrors.slice(0, 10).forEach(function (e) {
      console.log("    customer=%s: seq %d (at %s) came AFTER seq %d (at %s)",
        e.customer, e.currSeq, new Date(e.currAt).toISOString(),
        e.prevSeq, new Date(e.prevAt).toISOString());
    });
    if (orderErrors.length > 10) console.log("    ... and %d more", orderErrors.length - 10);

    // --- 5. WORKER DISTRIBUTION ---
    console.log("\n--- 5. WORKER DISTRIBUTION ---");
    var workerCounts = {};
    rows.forEach(function (r) {
      if (!workerCounts[r.worker_id]) workerCounts[r.worker_id] = 0;
      workerCounts[r.worker_id]++;
    });
    Object.keys(workerCounts).sort().forEach(function (w) {
      console.log("  %s: %d messages", w, workerCounts[w]);
    });
    console.log("  Total workers: %d", Object.keys(workerCounts).length);

    // --- 6. SAME CUSTOMER ON MULTIPLE WORKERS? ---
    console.log("\n--- 6. CUSTOMER-WORKER EXCLUSIVITY ---");
    var customerWorkers = {};
    rows.forEach(function (r) {
      var key = r.customer_id + "|" + r.instance_id;
      if (!customerWorkers[key]) customerWorkers[key] = new Set();
      customerWorkers[key].add(r.worker_id);
    });
    var multiWorkerCustomers = [];
    Object.keys(customerWorkers).forEach(function (key) {
      if (customerWorkers[key].size > 1) {
        multiWorkerCustomers.push({
          customer: key,
          workers: Array.from(customerWorkers[key]),
        });
      }
    });
    var exclusivityPass = multiWorkerCustomers.length === 0;
    console.log("  Customers on >1 worker: %d", multiWorkerCustomers.length);
    console.log("  Result:                 %s", exclusivityPass ? "PASS" : "WARN (rebalance happened)");
    multiWorkerCustomers.slice(0, 5).forEach(function (c) {
      console.log("    %s → workers: %s", c.customer, c.workers.join(", "));
    });

    // --- 7. PENDING ---
    console.log("\n--- 7. PENDING IN QUEUES ---");
    checkPendingQueues(function (pending) {
      var pendingPass = pending === 0;
      if (pending === -1) {
        console.log("  Pending:  (could not check)");
        console.log("  Result:   SKIP");
        pendingPass = true; // don't fail on this
      } else if (pending === 0) {
        console.log("  Pending:  0");
        console.log("  Result:   PASS");
      } else {
        console.log("  Pending:  %d messages still in queues", pending);
        console.log("  Result:   WAIT");
      }

      // --- FINAL ---
      var allPass = countPass && dupePass && missingPass && orderPass;

      console.log("\n" + "=".repeat(60));
      if (allPass && pendingPass) {
        console.log("  FINAL RESULT: ALL CHECKS PASSED");
        console.log("  %d messages, %d customers, %d workers — zero loss, zero duplicates, perfect order",
          rows.length, customersChecked, Object.keys(workerCounts).length);
      } else if (!allPass) {
        console.log("  FINAL RESULT: SOME CHECKS FAILED");
        if (missing.length > 0 && pending > 0) {
          console.log("  %d missing but %d still pending — try again later", missing.length, pending);
        }
      } else {
        console.log("  FINAL RESULT: WAIT — %d messages still processing", pending);
      }
      console.log("=".repeat(60));

      pgClient.end();
      process.exit(allPass && pendingPass ? 0 : 1);
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
          if (parts.length === 4 && parts[1] === "webhooks") pending += (q.messages || 0);
          if (parts.length === 3 && parts[0] === "webhooks" && parts[1] === "raw") pending += (q.messages || 0);
        });
        callback(pending);
      } catch (e) { callback(-1); }
    });
  });
  req.on("error", function () { callback(-1); });
  req.end();
}
