var http = require("http");

// ============================================================
// WEBHOOK SIMULATOR
// Sends fake webhooks to the API to test the full pipeline
// Each customer gets sequential sequence numbers to verify ordering
// ============================================================

// Track sequence per customer (by remoteJid)
var sequences = {};
function nextSeq(jid) {
  if (!jid) return null;
  if (!sequences[jid]) sequences[jid] = 0;
  sequences[jid]++;
  return sequences[jid];
}

var webhooks = [
  // Instance 1234 (tenant xpto) - customer bat1: 3 messages in order
  { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Olá, preciso de ajuda" },
  { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Meu pedido atrasou" },
  { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Pedido #12345" },

  // Instance 1234 (tenant xpto) - customer cli2: 2 messages
  { instanceId: "1234", event: "message.received", remoteJid: "5511999990002@s.whatsapp.net", body: "Quero fazer um pedido" },
  { instanceId: "1234", event: "message.received", remoteJid: "5511999990002@s.whatsapp.net", body: "Pizza grande" },

  // Instance 1231 (tenant xpto) - customer cli3: 1 message
  { instanceId: "1231", event: "message.received", remoteJid: "5511999990003@s.whatsapp.net", body: "Boa tarde" },

  // Instance abcde (tenant tomat) - customer mango: 2 messages
  { instanceId: "abcde", event: "message.received", remoteJid: "5511999990004@s.whatsapp.net", body: "Hello from mango" },
  { instanceId: "abcde", event: "message.received", remoteJid: "5511999990004@s.whatsapp.net", body: "Second message mango" },

  // Instance abcde (tenant tomat) - customer kiwi: 1 message
  { instanceId: "abcde", event: "message.received", remoteJid: "5511999990005@s.whatsapp.net", body: "Kiwi here" },

  // Instance goiaba (tenant tomat) - customer uva: 2 messages
  { instanceId: "goiaba", event: "message.received", remoteJid: "5511999990006@s.whatsapp.net", body: "Uva message 1" },
  { instanceId: "goiaba", event: "message.received", remoteJid: "5511999990006@s.whatsapp.net", body: "Uva message 2" },

  // System events (no customer, no sequence)
  { instanceId: "1234", event: "instance.connected" },
  { instanceId: "abcde", event: "instance.status", status: "online" },

  // Unknown customer (will be auto-created): 1 message
  { instanceId: "1234", event: "message.received", remoteJid: "5511888880001@s.whatsapp.net", body: "New customer!" },

  // More bat1 messages to test ordering with higher sequence
  { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Ainda estou esperando" },
  { instanceId: "1234", event: "message.received", remoteJid: "5511999990001@s.whatsapp.net", body: "Por favor respondam" },
];

// Assign sequence numbers per customer
webhooks.forEach(function (w) {
  w.sequence = nextSeq(w.remoteJid);
});

var sent = 0;

function sendNext() {
  if (sent >= webhooks.length) {
    console.log("\n[*] All %d webhooks sent!", webhooks.length);
    console.log("[*] Sequences assigned:");
    Object.keys(sequences).forEach(function (jid) {
      console.log("    %s → %d messages", jid, sequences[jid]);
    });
    console.log("\n[*] To verify ordering, run:");
    console.log('    docker exec -i postgres psql -U admin -d webhook_messages -c "SELECT * FROM v_order_check;"');
    console.log("\n[*] To see worker distribution:");
    console.log('    docker exec -i postgres psql -U admin -d webhook_messages -c "SELECT * FROM v_worker_distribution;"');
    console.log("\n[*] To see all messages in order:");
    console.log('    docker exec -i postgres psql -U admin -d webhook_messages -c "SELECT id, tenant_id, instance_id, customer_id, sequence, body, worker_id, processed_at FROM processed_messages ORDER BY customer_id, instance_id, processed_at;"');
    return;
  }

  var webhook = webhooks[sent];
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
      var seqStr = webhook.sequence ? " seq=" + webhook.sequence : "";
      console.log("[%d] %s %s%s → %s", sent + 1, webhook.instanceId, webhook.event, seqStr, data);
      sent++;
      // Small delay between sends to simulate real traffic
      setTimeout(sendNext, 100);
    });
  });

  req.on("error", function (err) {
    console.error("[!] Request failed:", err.message);
    sent++;
    setTimeout(sendNext, 500);
  });

  req.write(body);
  req.end();
}

console.log("[*] Sending %d webhooks to http://localhost:3001/webhook\n", webhooks.length);
sendNext();
