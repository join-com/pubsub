import { logger, reportError } from '@join-com/gcloud-logger-trace'
import * as trace from '@join-com/node-trace'
import { PubSub, Topic, Subscription, Message } from '@google-cloud/pubsub'
import { DataParser } from './DataParser'

export interface ParsedMessage<T = unknown> extends Message {
  dataParsed: T
}

export interface Options {
  ackDeadlineSeconds?: number
}

export class Subscriber<T = unknown> {
  private readonly topic: Topic
  private readonly subscription: Subscription
  private readonly options: Options

  constructor(
    topicName: string,
    subscriptionName: string,
    client: PubSub,
    options?: Options
  ) {
    this.topic = client.topic(topicName)
    this.subscription = this.topic.subscription(subscriptionName)
    this.options = options || {}
  }

  public async initialize() {
    try {
      await this.initializeTopic(this.topic)
      await this.initializeSubscription(this.subscription, this.options)
    } catch (e) {
      reportError(e)
      process.exit(1)
    }
  }

  public start(asyncCallback: (msg: ParsedMessage<T>) => Promise<void>) {
    this.subscription.on('error', reportError)
    this.subscription.on('message', this.processMsg(asyncCallback))
    logger.info(
      `PubSub: Subscription ${this.subscription.name} is started for topic ${this.topic.name}`
    )
  }

  private logMessage(message: Message, dataParsed: T) {
    const filteredMessage = {
      id: message.id,
      ackId: message.ackId,
      attributes: message.attributes,
      publishTime: message.publishTime,
      received: message.received
    }

    logger.info(
      `PubSub: Got message on topic: ${this.topic.name} with subscription: ${this.subscription.name} with data:`,
      { filteredMessage, dataParsed }
    )
  }

  private parseData(message: Message): T {
    const dataParser = new DataParser()
    const dataParsed = dataParser.parse(message.data)
    const traceContextName = trace.getTraceContextName()
    const traceId = dataParsed[traceContextName]
    trace.start(traceId)

    this.logMessage(message, dataParsed)
    delete dataParsed[traceContextName]
    return dataParsed
  }

  private processMsg(asyncCallback: (msg: ParsedMessage<T>) => Promise<void>) {
    return async (message: Message) => {
      try {
        const dataParsed = this.parseData(message)
        const messageParsed = Object.assign(message, { dataParsed })
        await asyncCallback(messageParsed)
      } catch (e) {
        message.nack()
        reportError(e)
      }
    }
  }

  private async initializeTopic(topic: Topic) {
    const [exist] = await topic.exists()
    logger.info(
      `PubSub: Topic ${topic.name} ${exist ? 'exists' : 'does not exist'}`
    )

    if (!exist) {
      await topic.create()
      logger.info(`PubSub: Topic ${topic.name} is created`)
    }
  }

  private async initializeSubscription(
    subscription: Subscription,
    options: Options
  ) {
    const [exist] = await subscription.exists()
    logger.info(
      `PubSub: Subscription ${subscription.name} ${
        exist ? 'exists' : 'does not exist'
      }`
    )

    if (!exist) {
      await subscription.create(options)
      logger.info(`PubSub: Subscription ${subscription.name} is created`)
    }
  }
}
