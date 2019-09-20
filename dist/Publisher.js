"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const gcloud_logger_trace_1 = require("@join-com/gcloud-logger-trace");
const trace = require("@join-com/node-trace");
class Publisher {
    constructor(topicName, client) {
        this.topicName = topicName;
        this.client = client;
        this.topic = this.client.topic(topicName);
        this.publisher = this.topic.publisher();
    }
    async start() {
        try {
            await this.createTopicIfNotExist();
        }
        catch (e) {
            gcloud_logger_trace_1.reportError(e);
            process.exit(1);
        }
    }
    async publishMsg(data) {
        const traceContext = trace.getTraceContext();
        const dataWithTrace = Object.assign(data, {
            [trace.getTraceContextName()]: traceContext
        });
        const strMsg = this.convertMessage(dataWithTrace);
        const messageId = await this.publisher.publish(strMsg);
        gcloud_logger_trace_1.logger.info(`PubSub: send message. Topic: ${this.topicName}. With data:`, {
            data,
            messageId
        });
        return messageId;
    }
    convertMessage(obj) {
        return Buffer.from(JSON.stringify(obj));
    }
    async createTopicIfNotExist() {
        const [topicExist] = await this.topic.exists();
        gcloud_logger_trace_1.logger.info(`PubSub: Topic ${this.topic.name} ${topicExist ? 'exists' : 'does not exist'}`);
        if (!topicExist) {
            await this.client.createTopic(this.topic.name);
            gcloud_logger_trace_1.logger.info(`PubSub: Topic ${this.topic.name} is created`);
        }
        gcloud_logger_trace_1.logger.info(`PubSub: started for topic ${this.topic.name}`);
    }
}
exports.Publisher = Publisher;
