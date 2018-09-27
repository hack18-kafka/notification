const kafka = require('kafka-node');
const ConsumerGroup = require('kafka-node').ConsumerGroup;
const moment = require('moment');

const pushClient = require('./pushClient');
const config = require('./config');



const client = new kafka.KafkaClient({kafkaHost: config.kafkaHost});

var consumerOptions = {
  host: config.kafkaHost,
  groupId: 'notificationGroup',
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
        value = toLowerKey(JSON.parse(message.value));
    } catch (error) {
        console.log('Received invalid JSON Data: ' + message.value);
    }
    
    if (value) {
        let notificationMessage = createNotificationMessage(message.topic, value);

        if (notificationMessage == null) {
            console.log(`Unable to construct notification message for topic ${message.topic}`);
        } else {
            let notification = {
                'userId': value.userid,
                'message': notificationMessage
            }
        
            sendNotification(notification);
        }
    }
});

const createNotificationMessage = (topic, value) => {
    let notificationMessage = null;

    if (topic.toUpperCase() === config.flight_topic) {
        notificationMessage = createFlightIssueMessage(value);
    } else if (topic.toUpperCase() === config.social_topic) {
        notificationMessage = createSocialEventMessage(value);
    } else if (topic.toUpperCase() === config.weather_topic) {
        notificationMessage = createWeatherEventMessage(value);
    } else {
        console.log ('undefined topic name: ' + topic);
    }

    return notificationMessage;
}

const createFlightIssueMessage = (value) => {
    let notificationMessage = `Your flight ${value.flightid} to ${value.flightdestination} on ${formatDate(value.flightdate)} is `;
    
    if (value.flightstatus.toUpperCase() === 'DELAYED') {
        notificationMessage = notificationMessage + `delayed for ${value.flightdelay} minutes.`;
    } else {
        notificationMessage = notificationMessage + `cancelled.`;
    }

    if (value.flightcompanyhelplink != null) {
        notificationMessage = notificationMessage +  ` You can get help at this site ${value.flightcompanyhelplink}`;    
    }

    return notificationMessage;
}

const createSocialEventMessage = (value) => {
    let message = ((value.eventmood > 0 ) ? 'Information: ' : 'Attention: ') + `In ${value.city} will be a ${value.eventtype} `;
    
    if (value.eventend == null) {
        message = message + `at ${formatDate(value.eventbegin)}`;
    } else {
        message = message + `from ${formatDate(value.eventbegin)} to ${formatDate(value.eventend)}`;
    }
    return message;
};

const createWeatherEventMessage = (value) => {
    let message = `Attention - ${value.weathertype} warning for ${value.city} `;
    
    if (value.weatherend == null) {
        message = message + `at ${formatDate(value.weatherbegin)}: `;
    } else {
        message = message + `from ${formatDate(value.weatherbegin)} to ${formatDate(value.weatherend)}: `;
    }
    message = message + value.weathermessage;
    return message;
}


consumerGroup.on("error", (error) => console.error('received error' + error));

const sendNotification = (notification) => {
    //HACK to accept only a real test user
    if (notification.userId.toUpperCase() === config.testuser.toUpperCase()) {
        //Hack to check how many notifications would have been sent
        if (config.notificationStatus) {
            pushClient.sendPushNotification(notification.userId, notification.message).then((response) => {
                //insertNotification(response.data[0].id, 0, notification.userId, response.data[0].status);
            }).catch(e => console.log(e.message));
            
        } else {
            console.log('Notification was not send due config: ' + notification.message);
        }

    } else {
        console.log('Received notification, but did not send it: ' + JSON.stringify(notification));
    }
};

const toLowerKey = (o) => {
    return Object.keys(o).reduce((c, k) => (c[k.toLowerCase()] = o[k], c), {});
}

const formatDate = (date) => {
    return moment(date).format('DD.MM.YYYY');
}


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
