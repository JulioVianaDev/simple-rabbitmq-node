var amqp = require("amqplib/callback_api");

amqp.connect("amqp://admin:admin@localhost:5672", function (err, conn) {
  if (err) {
    console.error("Connection failed:", err);
    process.exit(1);
  }

  conn.createChannel(function (err, ch) {
    if (err) {
      console.error("Channel creation failed:", err);
      conn.close();
      process.exit(1);
    }

    var q = "hello";
    var msg = "Hello World 123!";

    ch.assertQueue(q, { durable: false });
    ch.sendToQueue(q, Buffer.from(msg)); // Fixed: Use Buffer.from() instead of new Buffer()
    console.log(" [x] Sent %s", msg);
  });

  setTimeout(function () {
    conn.close();
    process.exit(0);
  }, 500);
});
