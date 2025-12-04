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

// Function to check message ordering for all instances
function checkAllInstancesOrdering() {
  console.log("\n[*] Checking message ordering for all instances...\n");

  var query = `
    SELECT DISTINCT instance_id 
    FROM consumed_messages 
    ORDER BY instance_id
  `;

  pgClient.query(query, function (err, result) {
    if (err) {
      console.error("[!] Error fetching instances:", err.message);
      pgClient.end();
      return;
    }

    if (result.rows.length === 0) {
      console.log("[*] No messages found in database.");
      pgClient.end();
      return;
    }

    console.log(
      "[*] Found %d instance(s) with messages\n",
      result.rows.length
    );

    var instances = result.rows.map(function (row) {
      return row.instance_id;
    });

    checkInstancesSequentially(instances, 0);
  });
}

// Function to check instances one by one
function checkInstancesSequentially(instances, index) {
  if (index >= instances.length) {
    showSummary();
    return;
  }

  var instanceId = instances[index];
  checkInstanceOrdering(instanceId, function () {
    checkInstancesSequentially(instances, index + 1);
  });
}

// Function to check message ordering for a specific instance
function checkInstanceOrdering(instanceId, callback) {
  console.log("=".repeat(80));
  console.log("Instance: %s", instanceId);
  console.log("=".repeat(80));

  // Get all messages for this instance ordered by sequence
  var query = `
    SELECT 
      queue_name,
      message_sequence,
      consumed_at,
      message_content,
      worker_id
    FROM consumed_messages
    WHERE instance_id = $1
    ORDER BY queue_name, message_sequence NULLS LAST, consumed_at
  `;

  pgClient.query(query, [instanceId], function (err, result) {
    if (err) {
      console.error("[!] Error:", err.message);
      if (callback) callback();
      return;
    }

    if (result.rows.length === 0) {
      console.log("[*] No messages found for this instance.\n");
      if (callback) callback();
      return;
    }

    // Group by queue
    var queues = {};
    result.rows.forEach(function (row) {
      var queueName = row.queue_name;
      if (!queues[queueName]) {
        queues[queueName] = [];
      }
      queues[queueName].push(row);
    });

    // Check ordering for each queue
    Object.keys(queues).forEach(function (queueName) {
      var messages = queues[queueName];
      console.log("\n  Queue: %s", queueName);
      console.log("  " + "-".repeat(76));

      var isOrdered = true;
      var lastSequence = null;
      var lastConsumedAt = null;

      messages.forEach(function (msg, idx) {
        var sequence = msg.message_sequence;
        var consumedAt = new Date(msg.consumed_at);
        var status = "";

        if (sequence !== null) {
          if (lastSequence !== null && sequence <= lastSequence) {
            status = " ❌ OUT OF ORDER";
            isOrdered = false;
          } else if (
            lastConsumedAt !== null &&
            consumedAt < lastConsumedAt
          ) {
            status = " ⚠️  TIMESTAMP OUT OF ORDER";
            isOrdered = false;
          } else {
            status = " ✓";
          }
        }

        console.log(
          "  [%d] Sequence: %s | Consumed: %s%s",
          idx + 1,
          sequence !== null ? sequence : "N/A",
          consumedAt.toISOString(),
          status
        );

        lastSequence = sequence;
        lastConsumedAt = consumedAt;
      });

      console.log("  " + "-".repeat(76));
      if (isOrdered) {
        console.log("  ✅ Queue is ORDERED (%d messages)", messages.length);
      } else {
        console.log("  ❌ Queue has OUT OF ORDER messages (%d messages)", messages.length);
      }
    });

    console.log();
    if (callback) callback();
  });
}

// Function to show summary statistics
function showSummary() {
  console.log("\n" + "=".repeat(80));
  console.log("SUMMARY STATISTICS");
  console.log("=".repeat(80) + "\n");

  var query = `
    SELECT 
      instance_id,
      queue_name,
      COUNT(*) as total_messages,
      COUNT(DISTINCT worker_id) as workers_processed,
      MIN(consumed_at) as first_message,
      MAX(consumed_at) as last_message
    FROM consumed_messages
    GROUP BY instance_id, queue_name
    ORDER BY instance_id, queue_name
  `;

  pgClient.query(query, function (err, result) {
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

    var totalMessages = 0;
    result.rows.forEach(function (row) {
      totalMessages += parseInt(row.total_messages);
      console.log("Instance: %s | Queue: %s", row.instance_id, row.queue_name);
      console.log("  Total Messages: %d", row.total_messages);
      console.log("  Workers: %d", row.workers_processed);
      console.log("  First: %s", new Date(row.first_message).toISOString());
      console.log("  Last:  %s", new Date(row.last_message).toISOString());
      console.log();
    });

    console.log("=".repeat(80));
    console.log("Total Messages Processed: %d", totalMessages);
    console.log("=".repeat(80) + "\n");

    pgClient.end();
  });
}

// Main execution
console.log("[*] Connecting to PostgreSQL...");
pgClient.connect(function (err) {
  if (err) {
    console.error("[!] PostgreSQL connection failed:", err.message);
    console.log(
      "[*] Make sure PostgreSQL is running and accessible at %s:%d",
      PG_CONFIG.host,
      PG_CONFIG.port
    );
    process.exit(1);
  }

  console.log("[*] Connected to PostgreSQL\n");
  checkAllInstancesOrdering();
});

// Handle errors
pgClient.on("error", function (err) {
  console.error("[!] PostgreSQL error:", err.message);
  process.exit(1);
});

