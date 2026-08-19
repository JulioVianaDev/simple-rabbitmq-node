var http = require("http");
var crypto = require("crypto");

// ============================================================
// INTEGRITY TEST
//
// Sends exactly 1 message per customer, each with a unique messageId.
// After processing, run verify-integrity.js to check:
//   - No duplicates (each messageId appears exactly once)
//   - No lost messages (all messageIds are in the database)
//   - Correct count (N customers = N rows in database)
//
// Usage:
//   node test-integrity.js 105    → 105 customers, 1 msg each
//   node test-integrity.js 500    → 500 customers
//   node test-integrity.js 1000   → 1000 customers
//
// Config: MAX_CONSUMERS_PER_WORKER should be less than the customer count
//         to test the rotation behavior (e.g., 50 for 105 customers).
// ============================================================

var TOTAL_CUSTOMERS = parseInt(process.argv[2]) || 105;
var TEST_ID = crypto.randomBytes(4).toString("hex"); // unique per test run

var webhooks = [];
var manifest = []; // track what we sent for verification

for (var i = 1; i <= TOTAL_CUSTOMERS; i++) {
  var phone = "55119" + String(i).padStart(8, "0");
  var messageId = TEST_ID + "-" + i;

  webhooks.push({
    instanceId: "1234",
    event: "message.received",
    remoteJid: phone + "@s.whatsapp.net",
    body: messageId, // use body to carry the unique messageId
    sequence: 1,
    testId: TEST_ID,
    messageId: messageId,
  });

  manifest.push(messageId);
}

// Save manifest to file so verify-integrity.js can check against it
var fs = require("fs");
var manifestFile = "test-manifest-" + TEST_ID + ".json";
fs.writeFileSync(manifestFile, JSON.stringify({
  testId: TEST_ID,
  totalCustomers: TOTAL_CUSTOMERS,
  messageIds: manifest,
  createdAt: new Date().toISOString(),
}, null, 2));

console.log("[*] Test ID: %s", TEST_ID);
console.log("[*] Customers: %d", TOTAL_CUSTOMERS);
console.log("[*] Manifest saved to: %s", manifestFile);
console.log("[*] Each customer gets exactly 1 message with a unique messageId\n");

var sent = 0;
var completed = 0;
var errors = 0;
var CONCURRENCY = 20;

function sendOne(index) {
  var webhook = webhooks[index];
  var body = JSON.stringify(webhook);

  var req = http.request({
    hostname: "localhost",
    port: 3001,
    path: "/webhook",
    method: "POST",
    headers: { "Content-Type": "application/json" },
  }, function (res) {
    var data = "";
    res.on("data", function (chunk) { data += chunk; });
    res.on("end", function () {
      completed++;
      if (completed % 50 === 0 || completed === webhooks.length) {
        console.log("[%d/%d] sent (%d errors)", completed, webhooks.length, errors);
      }
      scheduleNext();
    });
  });

  req.on("error", function (err) {
    errors++;
    completed++;
    if (errors <= 5) console.error("[!] Request failed:", err.message);
    scheduleNext();
  });

  req.write(body);
  req.end();
}

function scheduleNext() {
  if (sent < webhooks.length) {
    var idx = sent++;
    sendOne(idx);
  } else if (completed >= webhooks.length) {
    console.log("\n[*] All %d webhooks sent! (%d errors)", webhooks.length, errors);
    console.log("[*] Test ID: %s", TEST_ID);
    console.log("[*] Now wait for all messages to be processed, then run:");
    console.log("    node verify-integrity.js %s", manifestFile);
  }
}

console.log("[*] Sending %d webhooks to http://localhost:3001/webhook (concurrency: %d)\n", webhooks.length, CONCURRENCY);

for (var j = 0; j < Math.min(CONCURRENCY, webhooks.length); j++) {
  var idx = sent++;
  sendOne(idx);
}
