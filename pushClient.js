const axios = require('axios');
const querystring = require('querystring');

const config = require('./config');

axios.defaults.baseURL = config.baseURL;

const getOauthToken = async () => {
    try {
        
        const headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-ProxyPass' : 'entity-services',
        }

    
        const data = {
            client_id: config.client_id,
            client_secret: config.client_secret,
            grant_type: 'client_credentials'
        }

        return await axios.post('/oauth2/token', querystring.stringify(data), {headers: headers});
    } catch (error) {
        throw new Error(`Unable to get an authentication token. Reason ${error}`);
    }
    
};

const sendMessage = async (user, message, accessToken) => {
    try {
        const headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'X-ProxyPass' : 'entity-services',
            'authorization': 'Bearer ' + accessToken.access_token
        }

        const data = {
            users: [user],
            components: config.components,
            event_type: 'message',
            payload: {message: message}
        }

        return await axios.post('/events', JSON.stringify(data), {headers: headers});
    } catch (error) {
        throw new Error(`Unable to send notification. Reason ${error}`);
    }
};


const sendPushNotification = async (user, message) => {
    const authToken = await getOauthToken();

    const response =  await sendMessage(user, message, authToken.data);

    console.log(response);
    return response;
};

module.exports = {getOauthToken, sendMessage, sendPushNotification};
