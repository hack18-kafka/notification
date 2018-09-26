const kafka = require('kafka-node');
const config = require('./config');

const client = new kafka.KafkaClient({kafkaHost: '127.0.0.1:9092'});
let producer = new kafka.Producer(client);

let message = {
    userId: config.testuser,
    flightIssue: {
        flightNumber: 'LX123',
        flightCompany: 'Swiss',
        flightDestination: 'Barcelona',
        flightDate: '26.09.2018',
        flightStatus: 'cancelled',
        flightDelay: 2,
        flightCompanyHelpLink: 'https://axa.ch'
    }
};

producer.on('ready', () => {
    console.log('producer is ready');
    producer.send([{ topic: 'flight_delay', messages: JSON.stringify(message)}], (err, data) => {
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


