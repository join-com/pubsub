import {
  CreateSubscriptionOptions,
  IAM,
  Message,
  PubSub,
  Subscription,
  Topic
} from '@google-cloud/pubsub'
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
  deadLetterPolicy?: {
    maxDeliveryAttempts?: number
  }
  // TODO validate gcloudProject is given when isDeadLetterPolicyEnabled?
  gcloudProject?: {
    name: string
    id: number
  }
}

export class Subscriber<T = unknown> {
  private readonly topic: Topic
  private readonly subscription: Subscription

  private readonly deadLetterTopicName?: string
  private readonly deadLetterTopic?: Topic
  private readonly deadLetterSubscriptionName?: string
  private readonly deadLetterSubscription?: Subscription

  constructor(
    readonly topicName: string,
    readonly subscriptionName: string,
    pubsubClient: PubSub,
    private readonly options: ISubscriptionOptions = {},
    private readonly taskExecutor: ITaskExecutor = new DefaultTaskExecutor()
  ) {
    this.topic = pubsubClient.topic(topicName)
    this.subscription = this.topic.subscription(
      subscriptionName,
      this.getBaseOptions()
    )

    if (this.isDeadLetterPolicyEnabled()) {
      this.deadLetterTopicName = `${subscriptionName}-dead-letters`
      this.deadLetterTopic = pubsubClient.topic(this.deadLetterTopicName)
      this.deadLetterSubscriptionName = `${subscriptionName}-dead-letters-subscription`
      this.deadLetterSubscription = this.deadLetterTopic.subscription(
        this.deadLetterSubscriptionName
      )
    }
  }

  public async initialize() {
    try {
      await this.initializeTopic(this.topicName, this.topic)
      await this.initializeDeadLetterTopic()

      await this.initializeSubscription(
        this.subscriptionName,
        this.subscription,
        this.getInitializationOptions()
      )
      await this.initializeDeadLetterSubscription()
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

  private async initializeTopic(topicName: string, topic: Topic) {
    const [exist] = await topic.exists()
    logger.info(
      `PubSub: Topic ${topicName} ${exist ? 'exists' : 'does not exist'}`
    )

    if (!exist) {
      await topic.create()
      logger.info(`PubSub: Topic ${topicName} is created`)
    }
  }

  private async initializeSubscription(
    subscriptionName: string,
    subscription: Subscription,
    options?: CreateSubscriptionOptions
  ) {
    const [exist] = await subscription.exists()
    logger.info(
      `PubSub: Subscription ${subscriptionName} ${
        exist ? 'exists' : 'does not exist'
      }`
    )

    if (!exist) {
      await subscription.create(options)
      logger.info(`PubSub: Subscription ${subscriptionName} is created`)
    }
  }

  private async initializeDeadLetterTopic() {
    if (this.deadLetterTopicName && this.deadLetterTopic) {
      await this.initializeTopic(this.deadLetterTopicName, this.deadLetterTopic)
      await this.addPubsubServiceAccountRole(
        this.deadLetterTopic.iam,
        'roles/pubsub.publisher'
      )
    }
  }

  private async initializeDeadLetterSubscription() {
    if (this.deadLetterSubscriptionName && this.deadLetterSubscription) {
      await this.initializeSubscription(
        this.deadLetterSubscriptionName,
        this.deadLetterSubscription
      )
      await this.addPubsubServiceAccountRole(
        this.subscription.iam,
        'roles/pubsub.subscriber'
      )
    }
  }

  private async addPubsubServiceAccountRole(
    iam: IAM,
    role: 'roles/pubsub.subscriber' | 'roles/pubsub.publisher'
  ) {
    const gcloudProjectId =
      this.options.gcloudProject && this.options.gcloudProject.id
    const pubsubServiceAccount = `serviceAccount:service-${gcloudProjectId}@gcp-sa-pubsub.iam.gserviceaccount.com`

    await iam.setPolicy({
      bindings: [
        {
          members: [pubsubServiceAccount],
          role
        }
      ]
    })
  }

  private getBaseOptions() {
    const { deadLetterPolicy, ...baseOptions } = this.options
    return baseOptions
  }

  private getInitializationOptions() {
    const gcloudProjectName =
      this.options.gcloudProject && this.options.gcloudProject.name
    const deadLetterTopic = `projects/${gcloudProjectName}/topics/${this.deadLetterTopicName}`
    const options = {
      deadLetterPolicy: {
        ...this.options.deadLetterPolicy,
        deadLetterTopic
      }
    }
    return this.isDeadLetterPolicyEnabled() ? options : undefined
  }

  private isDeadLetterPolicyEnabled() {
    return Boolean(
      this.options.deadLetterPolicy &&
        this.options.deadLetterPolicy.maxDeliveryAttempts
    )
  }
}
