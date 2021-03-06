const kafka = require('kafka-node');
const config = require('./config');

const client = new kafka.KafkaClient({kafkaHost: '127.0.0.1:9092'});
let producer = new kafka.Producer(client);

let message = {
    USERID: config.testuser,
    FLIGHTID: 'LX1111',
    flightCompany: 'Swiss',
    FLIGHTDEPARTURE: 'Zurich',
    FLIGHTDESTINATION: 'Munich',
    FLIGHTDATE: '2018-09-27T12:23:01.123Z',
    FLIGHTSTATUS: 'delayed',
    FLIGHTDELAY: 2,
    FLIGHTCOMPANYHELPLINK: 'https://axa.ch'
};

producer.on('ready', () => {
    console.log('producer is ready');
    producer.send([{ topic: config.flight_topic, messages: JSON.stringify(message)}], (err, data) => {
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


