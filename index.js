const { S3EventConsumer } = require('./sqs-consumer');
require('dotenv').config()


const config = {
  region: process.env.AWS_REGION,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  queueUrl: process.env.AWS_QUEUE_URL
};

const consumer = new S3EventConsumer(config);
consumer.start();

process.on('SIGTERM', () => {
  consumer.stop();
});