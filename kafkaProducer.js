const kafka = require('kafka-node');
const config = require('./config');

const client = new kafka.KafkaClient({kafkaHost: '127.0.0.1:9092'});
let producer = new kafka.Producer(client);

let message = {
    userId: config.testuser,
    message: 'Test message'
};

producer.on('ready', () => {
    console.log('producer is ready');
    producer.send([{ topic: config.kafkaTopics, messages: JSON.stringify(message)}], (err, data) => {
        if (err) {
            console.log(err);
        } else {
            console.log(`send ${message} messages`);
        }
    });
});

producer.on('error', (err) => {
    console.log(err);
    process.exit();
})


