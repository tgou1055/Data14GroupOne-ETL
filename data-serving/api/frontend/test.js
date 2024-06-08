require('dotenv').config();
const axios = require('axios');

const apiUrl = process.env.API_URL;

const requestData = {
    s3_bucket: process.env.s3_bucket,
    s3_key: process.env.s3_key
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
