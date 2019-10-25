import { logger, reportError } from '@join-com/gcloud-logger-trace'
import * as trace from '@join-com/node-trace'
import {
  PubSub,
  Topic,
  Subscription,
  Message,
  SubscriptionOptions
} from '@google-cloud/pubsub'
import { DataParser } from './DataParser'

export interface ParsedMessage<T = unknown> extends Message {
  dataParsed: T
}

export type Options = SubscriptionOptions

export class Subscriber<T = unknown> {
  private readonly topic: Topic
  private readonly subscription: Subscription
  private readonly options: Options

  constructor(
    readonly topicName: string,
    readonly subscriptionName: string,
    client: PubSub,
    options?: Options
  ) {
    this.topic = client.topic(topicName)
    this.options = options || {}
    this.subscription = this.topic.subscription(subscriptionName, this.options)
  }

  public async initialize() {
    try {
      await this.initializeTopic()
      await this.initializeSubscription()
    } catch (e) {
      reportError(e)
      process.exit(1)
    }
  }

  public start(asyncCallback: (msg: ParsedMessage<T>) => Promise<void>) {
    this.subscription.on('error', reportError)
    this.subscription.on('message', this.processMsg(asyncCallback))
    logger.info(
      `PubSub: Subscription ${this.subscriptionName} is started for topic ${this.topicName}`
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
      `PubSub: Got message on topic: ${this.topicName} with subscription: ${this.subscriptionName} with data:`,
      { filteredMessage, dataParsed }
    )
  }

  private parseData(message: Message): T {
    const dataParser = new DataParser()
    const dataParsed = dataParser.parse(message.data)
    const attributes: { [key: string]: string } = message.attributes || {}
    const traceContextName = trace.getTraceContextName()
    const traceId = attributes[traceContextName]
    trace.start(traceId)

    this.logMessage(message, dataParsed)
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

  private async initializeTopic() {
    const [exist] = await this.topic.exists()
    logger.info(
      `PubSub: Topic ${this.topicName} ${exist ? 'exists' : 'does not exist'}`
    )

    if (!exist) {
      await this.topic.create()
      logger.info(`PubSub: Topic ${this.topicName} is created`)
    }
  }

  private async initializeSubscription() {
    const [exist] = await this.subscription.exists()
    logger.info(
      `PubSub: Subscription ${this.subscriptionName} ${
        exist ? 'exists' : 'does not exist'
      }`
    )

    if (!exist) {
      await this.subscription.create()
      logger.info(`PubSub: Subscription ${this.subscriptionName} is created`)
    }
  }
}
