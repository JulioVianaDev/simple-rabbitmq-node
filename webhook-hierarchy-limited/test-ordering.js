var http = require("http");
var crypto = require("crypto");
var fs = require("fs");

// ============================================================
// ORDERING + INTEGRITY TEST
//
// Each customer gets 3 messages with sequence 1, 2, 3.
// Messages are SHUFFLED randomly — simulating real traffic where
// customer A sends 2 messages, then 15 other customers send,
// then customer A sends the 3rd, etc.
//
// Verifies:
//   - All messages arrive (no loss, even with queue rotation)
//   - No duplicates
//   - Messages are processed IN ORDER per customer (seq 1 → 2 → 3)
//
// Usage:
//   node test-ordering.js 105    → 105 customers x 3 msgs = 315 total
//   node test-ordering.js 50     → 50 customers x 3 msgs = 150 total
// ============================================================

var TOTAL_CUSTOMERS = parseInt(process.argv[2]) || 105;
var MSGS_PER_CUSTOMER = 3;
var TEST_ID = crypto.randomBytes(4).toString("hex");

// Step 1: Generate all messages per customer in order
var messagesByCustomer = {};
var allMessages = [];

for (var i = 1; i <= TOTAL_CUSTOMERS; i++) {
  var phone = "55119" + String(i).padStart(8, "0");
  var jid = phone + "@s.whatsapp.net";
  messagesByCustomer[phone] = [];

  for (var seq = 1; seq <= MSGS_PER_CUSTOMER; seq++) {
    var messageId = TEST_ID + "-c" + i + "-s" + seq;
    var webhook = {
      instanceId: "1234",
      event: "message.received",
      remoteJid: jid,
      body: messageId,
      sequence: seq,
      testId: TEST_ID,
      messageId: messageId,
    };
    messagesByCustomer[phone].push(webhook);
    allMessages.push(webhook);
  }
}

// Step 2: Shuffle to simulate realistic traffic
// Strategy: for each customer, pick random positions for their 3 messages
// but maintain relative order (msg1 before msg2 before msg3 for the same customer)
//
// Algorithm: interleave customers randomly
// Take the next message from a random customer that still has unsent messages

var sendOrder = [];
var queues = {}; // phone -> array of pending messages (in order)
Object.keys(messagesByCustomer).forEach(function (phone) {
  queues[phone] = messagesByCustomer[phone].slice(); // copy
});

var phonesWithMessages = Object.keys(queues);

// Fisher-Yates-like interleaving
while (phonesWithMessages.length > 0) {
  // Pick a random customer
  var idx = Math.floor(Math.random() * phonesWithMessages.length);
  var phone = phonesWithMessages[idx];

  // Take their NEXT message (preserves per-customer order)
  sendOrder.push(queues[phone].shift());

  // If customer has no more messages, remove from pool
  if (queues[phone].length === 0) {
    phonesWithMessages.splice(idx, 1);
  }
}

// Step 3: Show the shuffle quality
var firstMsgPositions = {};
var lastMsgPositions = {};
sendOrder.forEach(function (msg, pos) {
  var custId = msg.messageId.split("-s")[0]; // TEST_ID-cN
  if (msg.sequence === 1) firstMsgPositions[custId] = pos;
  if (msg.sequence === MSGS_PER_CUSTOMER) lastMsgPositions[custId] = pos;
});

var gaps = [];
Object.keys(firstMsgPositions).forEach(function (custId) {
  gaps.push(lastMsgPositions[custId] - firstMsgPositions[custId]);
});
gaps.sort(function (a, b) { return a - b; });

console.log("[*] Test ID:     %s", TEST_ID);
console.log("[*] Customers:   %d", TOTAL_CUSTOMERS);
console.log("[*] Msgs/customer: %d", MSGS_PER_CUSTOMER);
console.log("[*] Total msgs:  %d", sendOrder.length);
console.log("[*] Shuffle quality:");
console.log("    Min gap between first and last msg of same customer: %d positions", gaps[0]);
console.log("    Max gap: %d positions", gaps[gaps.length - 1]);
console.log("    Median gap: %d positions", gaps[Math.floor(gaps.length / 2)]);
console.log("");

// Show first 20 messages to visualize the shuffle
console.log("[*] First 20 messages in send order:");
sendOrder.slice(0, 20).forEach(function (msg, i) {
  var custNum = msg.messageId.match(/c(\d+)/)[1];
  console.log("    [%d] customer=%s seq=%d", i + 1, custNum, msg.sequence);
});
console.log("    ...\n");

// Step 4: Save manifest
var manifestData = {
  testId: TEST_ID,
  totalCustomers: TOTAL_CUSTOMERS,
  msgsPerCustomer: MSGS_PER_CUSTOMER,
  totalMessages: sendOrder.length,
  messageIds: sendOrder.map(function (m) { return m.messageId; }),
  customerMessages: {}, // customerId -> [messageId1, messageId2, messageId3] in order
  createdAt: new Date().toISOString(),
};

Object.keys(messagesByCustomer).forEach(function (phone) {
  manifestData.customerMessages[phone] = messagesByCustomer[phone].map(function (m) {
    return { messageId: m.messageId, sequence: m.sequence };
  });
});

var manifestFile = "test-manifest-" + TEST_ID + ".json";
fs.writeFileSync(manifestFile, JSON.stringify(manifestData, null, 2));
console.log("[*] Manifest saved to: %s", manifestFile);

// Step 5: Send all webhooks
var sent = 0;
var completed = 0;
var errors = 0;
var CONCURRENCY = 1; // Send ONE AT A TIME to preserve the shuffled order into the raw queue

function sendOne(index) {
  var webhook = sendOrder[index];
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
      if (completed % 50 === 0 || completed === sendOrder.length) {
        console.log("[%d/%d] sent (%d errors)", completed, sendOrder.length, errors);
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
  if (sent < sendOrder.length) {
    var idx = sent++;
    sendOne(idx);
  } else if (completed >= sendOrder.length) {
    console.log("\n[*] All %d webhooks sent! (%d errors)", sendOrder.length, errors);
    console.log("[*] Test ID: %s", TEST_ID);
    console.log("[*] Wait for processing to complete, then run:");
    console.log("    node verify-ordering.js %s", manifestFile);
  }
}

console.log("[*] Sending %d webhooks ONE AT A TIME (order matters)\n", sendOrder.length);
sent++;
sendOne(0);
