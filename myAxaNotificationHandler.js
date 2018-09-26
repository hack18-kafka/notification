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

var topics = [config.kafkaTopics];

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
        let notification = {
            'userId': value.userId,
            'message': value.message
        }
    
        sendNotification(notification);
    }
});
 
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
