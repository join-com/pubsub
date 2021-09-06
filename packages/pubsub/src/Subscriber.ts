import {
  IAM,
  Message,
  Subscription,
  Topic,
  PubSub,
  SubscriptionOptions
} from '@google-cloud/pubsub'
import { logger } from '@join-com/gcloud-logger-trace'
import { reportError } from './reportError'
import * as trace from '@join-com/node-trace'
import { DataParser } from './DataParser'

export interface IParsedMessage<T = unknown> {
  dataParsed: T
  ack: () => void
  nack: () => void
}

export interface ISubscriptionOptions {
  ackDeadline?: number
  allowExcessMessages?: boolean
  maxMessages?: number
  maxStreams?: number
  maxDeliveryAttempts?: number
  minBackoffSeconds?: number
  maxBackoffSeconds?: number
  // TODO validate gcloudProject is given when isDeadLetterPolicyEnabled?
  gcloudProject?: {
    name: string
    id: number
  }
}

interface ISubscriptionRetryPolicy {
  minimumBackoff?: { seconds?: number }
  maximumBackoff?: { seconds?: number }
}

interface ISubscriptionDeadLetterPolicy {
  maxDeliveryAttempts?: number
  deadLetterTopic: string
}

interface ISubscriptionInitializationOptions {
  deadLetterPolicy: ISubscriptionDeadLetterPolicy | null
  retryPolicy: ISubscriptionRetryPolicy
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
    private readonly options: ISubscriptionOptions = {}
  ) {
    this.topic = pubsubClient.topic(topicName)
    this.subscription = this.topic.subscription(
      subscriptionName,
      this.getStartupOptions(options)
    )

    if (this.isDeadLetterPolicyEnabled()) {
      this.deadLetterTopicName = `${subscriptionName}-unack`
      this.deadLetterTopic = pubsubClient.topic(this.deadLetterTopicName)
      this.deadLetterSubscriptionName = `${subscriptionName}-unack`
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
      received: message.received,
      deliveryAttempt: message.deliveryAttempt
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
        const dataParsed = this.parseData(message)
        const messageParsed = Object.assign(message, { dataParsed })
        await asyncCallback(messageParsed)
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
    logger.info(`Reopened subscription ${this.subscription.name} after error`, {
      error
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
    options?: ISubscriptionInitializationOptions
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
    } else if (options) {
      await subscription.setMetadata(options)
      logger.info(`PubSub: Subscription ${subscriptionName} metadata updated`)
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

  private isDeadLetterPolicyEnabled() {
    return Boolean(this.options.maxDeliveryAttempts)
  }

  private getInitializationOptions(): ISubscriptionInitializationOptions {
    const options: ISubscriptionInitializationOptions = {
      deadLetterPolicy: null,
      retryPolicy: {}
    }

    if (this.options.minBackoffSeconds !== undefined) {
      options.retryPolicy.minimumBackoff = {
        seconds: this.options.minBackoffSeconds
      }
    }

    if (this.options.maxBackoffSeconds !== undefined) {
      options.retryPolicy.maximumBackoff = {
        seconds: this.options.maxBackoffSeconds
      }
    }

    if (this.isDeadLetterPolicyEnabled()) {
      const gcloudProjectName = this.options.gcloudProject?.name
      const deadLetterTopic = `projects/${gcloudProjectName}/topics/${this.deadLetterTopicName}`
      options.deadLetterPolicy = {
        maxDeliveryAttempts: this.options.maxDeliveryAttempts,
        deadLetterTopic
      }
    }

    return options
  }

  private getStartupOptions(
    options?: ISubscriptionOptions
  ): SubscriptionOptions {
    return {
      ackDeadline: options?.ackDeadline,
      flowControl: {
        allowExcessMessages: options?.allowExcessMessages,
        maxMessages: options?.maxMessages
      },
      streamingOptions: {
        maxStreams: options?.maxStreams
      }
    }
  }
}
