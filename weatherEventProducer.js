const kafka = require('kafka-node');
const config = require('./config');

const client = new kafka.KafkaClient({kafkaHost: '127.0.0.1:9092'});
let producer = new kafka.Producer(client);

let message = {
    USERID: config.testuser,
    CITY: 'Munich',
    WEATHERTYPE: 'earthquake',
    WEATHERMESSAGE: 'Be aware of possible earthquakes',
    WEATHERBEGIN: '2018-09-27T12:23:01.123Z',
    WEATHEREND: '2018-09-28T12:23:01.123Z'
};

producer.on('ready', () => {
    console.log('producer is ready');
    producer.send([{ topic: config.weather_topic, messages: JSON.stringify(message)}], (err, data) => {
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


