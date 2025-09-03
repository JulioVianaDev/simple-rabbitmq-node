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

    ch.assertQueue(q, { durable: false });
    ch.prefetch(1);
    console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q);

    ch.consume(
      q,
      function (msg) {
        console.log(" [x] Received %s", msg.content.toString());
      },
      { noAck: true }
    );
  });
});
