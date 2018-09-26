const kafka = require('kafka-node');
const ConsumerGroup = require('kafka-node').ConsumerGroup;

const pushClient = require('./pushClient');
const config = require('./config');



const client = new kafka.KafkaClient({kafkaHost: config.kafkaHost});

var consumerOptions = {
  host: config.kafkaHost,
  groupId: 'ExampleTestGroup',
  sessionTimeout: 15000,
  protocol: ['roundrobin'],
  fromOffset: 'earliest' // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
};

var topics = config.kafkaTopics;

var consumerGroup = new ConsumerGroup(Object.assign({id: 'consumer1'}, consumerOptions), topics);
 
consumerGroup.on("message", (message) => {
    console.log('Received message: ' + JSON.stringify(message));
    
    let value;
    
    try {
        value = JSON.parse(message.value);
    } catch (error) {
        console.log('Received invalid JSON Data: ' + message.value);
    }
    
    if (value) {
        let notificationMessage = createNotificationMessage(message.topic, value);

        if (notificationMessage == null) {
            console.log(`Unable to construct notification message for topic ${message.topic}`);
        } else {
            let notification = {
                'userId': value.userId,
                'message': notificationMessage
            }
        
            sendNotification(notification);
        }
    }
});

const createNotificationMessage = (topic, value) => {
    let notificationMessage = null;

    if (topic.toUpperCase() === 'FLIGHT_DELAY') {
        let flightIssue = value.flightIssue;

        notificationMessage = `Your flight ${flightIssue.flightNumber} to ${flightIssue.flightDestination} on ${flightIssue.flightDate} is `;
       
        if (flightIssue.flightStatus.toUpperCase() === 'DELAYED') {
            notificationMessage = notificationMessage + `delayed for ${flightIssue.flightDelay} hours.`;
        } else {
            notificationMessage = notificationMessage + `cancelled.`;
        }

        if (flightIssue.flightCompanyHelpLink != null) {
            notificationMessage = notificationMessage +  ` You can get help at this site ${flightIssue.flightCompanyHelpLink}`;    
        }
    } else if (topic.toUpperCase() === 'MYAXA-NOTIFICATION') {
        notificationMessage = value.message;
    } else {
        console.log ('undefined topic name: ' + topic);
    }

    return notificationMessage;
}

consumerGroup.on("error", (error) => console.error('received error' + error));

const sendNotification = async (notification) => {
    //HACK to accept only a real test user
    if (notification.userId.toUpperCase() === config.testuser.toUpperCase() && config.notificationStatus) {
        const response = await pushClient.sendPushNotification(notification.userId, notification.message);
        console.log('Response from sendPushNotifiction: ' + response);
    } else {
        console.log('Received notification, but did not send it: ' + JSON.stringify(notification));
    }
};

const shutdown = () => {
    consumerGroup.close(true, (err) => {
        console.log(err);
        process.exit();
    });
    console.log('Kafka consumer closed');
    process.exit(0);
};

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
