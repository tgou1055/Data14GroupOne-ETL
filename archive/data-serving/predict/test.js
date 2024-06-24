require('dotenv').config();
const axios = require('axios');

const apiUrl = process.env.API_URL;

/**
 * more tests data:
 * '4.6,3.6,1.0,0.2' -> prediction: 0
 * '6.7,3.1,4.4,1.4' -> prediction: 1
 * '5.8,2.7,5.1,1.9' -> prediction: 2
 */
const requestData = {
    data: '4.6,3.6,1.0,0.2'
};

axios.post(apiUrl, requestData, {
  headers: {
    'Content-Type': 'application/json'
  }
})
.then(response => {
  console.log(response.data.body);
})
.catch(error => {
  console.error('Error:', error);
});
