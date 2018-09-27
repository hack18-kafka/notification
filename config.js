
const weather_topic = 'merge_weather_streams';
const social_topic = 'merge_social_streams';
const flight_topic = 'join_user_flight_event';
const config = {

    baseURL : '',
    client_id: '',
    client_secret: '',
    components: ['874b81afacd923af6ab803a42d1752ae', '1f92bde4eac082442acbe614b7dd0577'],
    testuser: '5330B441-FE9B-442D-BCA7-868C5D9855C6',
    //kafkaHost: '127.0.0.1:2181',
    kafkaHost: 'kafka.service.ocean:9092',

    weather_topic: weather_topic,
    social_topic: social_topic,
    flight_topic: flight_topic,

    kafkaTopics: [flight_topic, social_topic, weather_topic],
    notificationStatus: false
}

module.exports = config;
