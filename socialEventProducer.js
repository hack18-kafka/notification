const kafka = require('kafka-node');
const config = require('./config');

const client = new kafka.KafkaClient({kafkaHost: '127.0.0.1:9092'});
let producer = new kafka.Producer(client);

let message = {
    USERID: config.testuser,
    CITY: 'Munich',
    EVENTTYPE: 'strike',
    EVENTMOOD: -1,
    EVENTBEGIN: '2018-09-27T12:23:01.123Z',
    EVENTEND: '2018-09-28T12:23:01.123Z'
};

producer.on('ready', () => {
    console.log('producer is ready');
    producer.send([{ topic: config.social_topic, messages: JSON.stringify(message)}], (err, data) => {
        if (err) {
            console.log(err);
        } else {
            console.log(`send ${message} messages`);
        }
        process.exit();
    });
});

producer.on('error', (err) => {
    console.log(err);
    process.exit();
})


