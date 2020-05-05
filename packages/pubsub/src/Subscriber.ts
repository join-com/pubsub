import { Message, PubSub, Subscription, Topic } from '@google-cloud/pubsub'
import { logger, reportError } from '@join-com/gcloud-logger-trace'
import * as trace from '@join-com/node-trace'
import { DataParser } from './DataParser'
import { DefaultTaskExecutor, ITaskExecutor } from './DefaultTaskExecutor'

export interface IParsedMessage<T = unknown> extends Message {
  dataParsed: T
}

export interface ISubscriptionOptions {
  ackDeadline?: number
  flowControl?: {
    allowExcessMessages?: boolean
    maxMessages?: number
  }
  streamingOptions?: {
    highWaterMark?: number
    maxStreams?: number
    timeout?: number
  }
}

export class Subscriber<T = unknown> {
  private readonly topic: Topic
  private readonly subscription: Subscription
  private readonly options: ISubscriptionOptions
  private readonly taskExecutor: ITaskExecutor

  constructor(
    readonly topicName: string,
    readonly subscriptionName: string,
    pubsubClient: PubSub,
    options?: ISubscriptionOptions,
    taskExecutor?: ITaskExecutor
  ) {
    this.topic = pubsubClient.topic(topicName)
    this.options = options || {}
    this.subscription = this.topic.subscription(subscriptionName, this.options)
    this.taskExecutor = taskExecutor || new DefaultTaskExecutor()
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

  public start(asyncCallback: (msg: IParsedMessage<T>) => Promise<void>) {
    this.subscription.on('error', this.processError)
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

  private processMsg(asyncCallback: (msg: IParsedMessage<T>) => Promise<void>) {
    return async (message: Message) => {
      try {
        const processAction = () => {
          const dataParsed = this.parseData(message)
          const messageParsed = Object.assign(message, { dataParsed })
          return asyncCallback(messageParsed)
        }
        await this.taskExecutor.execute(message.id, processAction)
      } catch (e) {
        message.nack()
        reportError(e)
      }
    }
  }

  private processError = async (error: Error) => {
    reportError(error)
    await this.subscription.close()
    this.subscription.open()
    logger.info('Reopened subscription after error', {
      error,
      name: this.subscription.name
    })
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
