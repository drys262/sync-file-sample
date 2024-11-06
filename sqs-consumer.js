const { Consumer } = require("sqs-consumer");
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { SQSClient } = require("@aws-sdk/client-sqs");
const winston = require("winston");
const fs = require("fs").promises;
const path = require("path");

const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: "error.log", level: "error" }),
    new winston.transports.File({ filename: "combined.log" }),
  ],
});

class S3EventConsumer {
  constructor(config) {
    this.sqsClient = new SQSClient({
      region: config.region,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
    });

    this.s3Client = new S3Client({
      region: config.region,
      credentials: {
        accessKeyId: config.accessKeyId,
        secretAccessKey: config.secretAccessKey,
      },
    });

    this.syncDir = path.join(process.cwd(), "sync");
    fs.mkdir(this.syncDir, { recursive: true }).catch((err) =>
      logger.error("Failed to create sync directory", err)
    );

    this.consumer = Consumer.create({
      queueUrl: config.queueUrl,
      handleMessage: async (message) => {
        await this.processMessage(message);
      },
      sqs: this.sqsClient,
      batchSize: 10,
      pollingWaitTimeMs: 20000,
    });

    this.consumer.on("error", (err) => {
      logger.error("Error in consumer", err);
    });

    this.consumer.on("processing_error", (err) => {
      logger.error("Processing error", err);
    });

    this.consumer.on("timeout_error", (err) => {
      logger.error("Timeout error", err);
    });
  }

  async processMessage(message) {
    try {
      console.log("Message", message);
      const body = JSON.parse(message.Body);
      console.log("BODY here", body);
      if (!body.Records || body.event === "s3:TestEvent") {
        return;
      }
      const records = body.Records;

      for (const record of records) {
        const eventName = record.eventName;
        const s3 = record.s3;

        logger.info("Processing S3 event", {
          eventName,
          bucket: s3.bucket.name,
          key: s3.object.key,
        });

        switch (true) {
          case eventName.startsWith("ObjectCreated"):
            await this.handleObjectCreated(s3);
            break;
          case eventName.startsWith("ObjectRemoved"):
            await this.handleObjectRemoved(s3);
            break;
          default:
            logger.warn("Unhandled event type", { eventName });
        }
      }
    } catch (err) {
      logger.error("Failed to process message", err);
      throw err;
    }
  }

  async handleObjectCreated(s3Data) {
    console.log("S3 data", s3Data);
    const bucket = s3Data.bucket.name;
    const key = s3Data.object.key;
    logger.info("Object created", {
      bucket: s3Data.bucket.name,
      key: s3Data.object.key,
    });

    try {
      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key,
      });

      const response = await this.s3Client.send(command);

      const filePath = path.join(this.syncDir, key);
      await fs.mkdir(path.dirname(filePath), { recursive: true });

      const chunks = [];
      for await (const chunk of response.Body) {
        chunks.push(chunk);
      }
      const fileContent = Buffer.concat(chunks);
      await fs.writeFile(filePath, fileContent);

      logger.info("File synchronized locally", {
        bucket,
        key,
        localPath: filePath,
      });
    } catch (err) {
      logger.error("Failed to download and save file", err);
      throw err;
    }
  }

  async handleObjectRemoved(s3Data) {
    const bucket = s3Data.bucket.name;
    const key = s3Data.object.key;

    logger.info("Object removed", {
      bucket,
      key,
    });

    try {
      const filePath = path.join(this.syncDir, key);

      await fs.unlink(filePath);

      const dirPath = path.dirname(filePath);
      try {
        await fs.rmdir(dirPath);
      } catch (err) {
        // Ignore error
      }

      logger.info("File removed from local sync", {
        bucket,
        key,
        localPath: filePath,
      });
    } catch (err) {
      logger.error("Failed to remove file", err);
      throw err;
    }
  }

  start() {
    this.consumer.start();
    logger.info("Consumer started");
  }

  stop() {
    this.consumer.stop();
    logger.info("Consumer stopped");
  }
}

module.exports = {
  S3EventConsumer,
};
