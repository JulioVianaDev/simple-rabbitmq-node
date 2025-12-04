var { Client } = require("pg");

// PostgreSQL Configuration
var PG_CONFIG = {
  host: "localhost",
  port: 5432,
  database: "webhook_messages",
  user: "admin",
  password: "admin",
};

var pgClient = new Client(PG_CONFIG);
var instanceFilter = process.argv[2]; // Optional: filter by instance ID

// Main execution
console.log("[*] Connecting to PostgreSQL...");
pgClient.connect(function (err) {
  if (err) {
    console.error("[!] PostgreSQL connection failed:", err.message);
    process.exit(1);
  }

  console.log("[*] Connected to PostgreSQL\n");

  var query = `
    SELECT 
      id,
      instance_id,
      queue_name,
      message_sequence,
      consumed_at,
      worker_id,
      LEFT(message_content, 50) as message_preview
    FROM consumed_messages
    ${instanceFilter ? "WHERE instance_id = $1" : ""}
    ORDER BY instance_id, queue_name, message_sequence NULLS LAST, consumed_at
    LIMIT 100
  `;

  var params = instanceFilter ? [instanceFilter] : [];

  pgClient.query(query, params, function (err, result) {
    if (err) {
      console.error("[!] Error:", err.message);
      pgClient.end();
      return;
    }

    if (result.rows.length === 0) {
      console.log("[*] No messages found.\n");
      pgClient.end();
      return;
    }

    console.log("=".repeat(120));
    console.log(
      "ID".padEnd(6) +
        "Instance".padEnd(20) +
        "Queue".padEnd(25) +
        "Seq".padEnd(6) +
        "Consumed At".padEnd(25) +
        "Worker".padEnd(20) +
        "Message Preview"
    );
    console.log("=".repeat(120));

    result.rows.forEach(function (row) {
      console.log(
        String(row.id).padEnd(6) +
          (row.instance_id || "N/A").padEnd(20) +
          (row.queue_name || "N/A").substring(0, 24).padEnd(25) +
          (row.message_sequence !== null
            ? String(row.message_sequence)
            : "N/A"
          ).padEnd(6) +
          new Date(row.consumed_at).toISOString().padEnd(25) +
          (row.worker_id || "N/A").substring(0, 19).padEnd(20) +
          (row.message_preview || "")
      );
    });

    console.log("=".repeat(120));
    console.log("\nTotal messages shown: %d", result.rows.length);
    console.log(
      "Usage: node view-messages.js [instanceId]  (to filter by instance)\n"
    );

    pgClient.end();
  });
});

pgClient.on("error", function (err) {
  console.error("[!] PostgreSQL error:", err.message);
  process.exit(1);
});

