var http = require("http");

// ============================================================
// WEBHOOK SIMULATOR (STRESS MODE)
// Generates many customers to test the MAX_CONSUMERS limit.
// Usage:
//   node simulate-webhooks.js              → 16 webhooks (normal test)
//   node simulate-webhooks.js 50           → 50 customers x 3 msgs each
//   node simulate-webhooks.js 600          → 600 customers (exceeds 500 limit)
//   node simulate-webhooks.js 10000        → 10k customers stress test
// ============================================================

var arg = process.argv[2];
var stressCustomers = parseInt(arg);
var mode = (stressCustomers > 0) ? "stress" : "normal";

var sequences = {};
function nextSeq(jid) {
  if (!jid) return null;
  if (!sequences[jid]) sequences[jid] = 0;
  sequences[jid]++;
  return sequences[jid];
}

var webhooks = [];

if (mode === "stress") {
  console.log("[*] STRESS MODE: generating %d customers x 3 messages each\n", stressCustomers);

  for (var c = 1; c <= stressCustomers; c++) {
    var phone = "55119" + String(c).padStart(8, "0");
    var jid = phone + "@s.whatsapp.net";
    for (var m = 1; m <= 3; m++) {
      webhooks.push({
        instanceId: "1234",
        event: "message.received",
        remoteJid: jid,
        body: "Customer " + c + " message " + m,
      });
    }
  }
} else {
  webhooks = [
    { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Ola, preciso de ajuda" },
    { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Meu pedido atrasou" },
    { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Pedido #12345" },
    { instanceId: "1234", event: "message.received", remoteJid: "5511999990002@s.whatsapp.net", body: "Quero fazer um pedido" },
    { instanceId: "1234", event: "message.received", remoteJid: "5511999990002@s.whatsapp.net", body: "Pizza grande" },
    { instanceId: "1231", event: "message.received", remoteJid: "5511999990003@s.whatsapp.net", body: "Boa tarde" },
    { instanceId: "abcde", event: "message.received", remoteJid: "5511999990004@s.whatsapp.net", body: "Hello from mango" },
    { instanceId: "abcde", event: "message.received", remoteJid: "5511999990004@s.whatsapp.net", body: "Second message mango" },
    { instanceId: "abcde", event: "message.received", remoteJid: "5511999990005@s.whatsapp.net", body: "Kiwi here" },
    { instanceId: "goiaba", event: "message.received", remoteJid: "5511999990006@s.whatsapp.net", body: "Uva message 1" },
    { instanceId: "goiaba", event: "message.received", remoteJid: "5511999990006@s.whatsapp.net", body: "Uva message 2" },
    { instanceId: "1234", event: "instance.connected" },
    { instanceId: "abcde", event: "instance.status", status: "online" },
    { instanceId: "1234", event: "message.received", remoteJid: "5511888880001@s.whatsapp.net", body: "New customer!" },
    { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Ainda estou esperando" },
    { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Por favor respondam" },
  ];
}

webhooks.forEach(function (w) {
  w.sequence = nextSeq(w.remoteJid);
});

var sent = 0;
var completed = 0;
var errors = 0;
var CONCURRENCY = mode === "stress" ? 50 : 1;

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
      if (completed % 500 === 0 || completed === webhooks.length) {
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
    if (mode === "stress") {
      console.log("[*] Unique customers: %d", Object.keys(sequences).length);
    }
    console.log("\n[*] To verify ordering:");
    console.log('    docker exec -i postgres psql -U admin -d webhook_messages -c "SELECT * FROM v_order_check;"');
  }
}

console.log("[*] Sending %d webhooks to http://localhost:3001/webhook (concurrency: %d)\n", webhooks.length, CONCURRENCY);

// Launch initial batch of concurrent requests
for (var i = 0; i < Math.min(CONCURRENCY, webhooks.length); i++) {
  var idx = sent++;
  sendOne(idx);
}
