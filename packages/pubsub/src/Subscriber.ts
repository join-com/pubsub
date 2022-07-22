import { IAM, Message, Subscription, Topic, PubSub, SubscriptionOptions } from '@google-cloud/pubsub'
import { createCallOptions } from './createCallOptions'
import { DataParser } from './DataParser'
import { ILogger } from './ILogger'

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

export interface ISubscriberOptions {
  topicName: string
  subscriptionName: string
  subscriptionOptions?: ISubscriptionOptions
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
  readonly topicName: string
  readonly subscriptionName: string

  private readonly topic: Topic
  private readonly subscription: Subscription

  private readonly deadLetterTopicName?: string
  private readonly deadLetterSubscriptionName?: string

  private readonly deadLetterTopic?: Topic
  private readonly deadLetterSubscription?: Subscription

  constructor(
    private readonly subscriberOptions: ISubscriberOptions,
    pubSubClient: PubSub,
    private readonly logger?: ILogger,
  ) {
    const { topicName, subscriptionName, subscriptionOptions } = subscriberOptions

    this.topicName = topicName
    this.subscriptionName = subscriptionName

    this.topic = pubSubClient.topic(topicName)
    this.subscription = this.topic.subscription(subscriptionName, this.getStartupOptions(subscriptionOptions))

    if (this.isDeadLetterPolicyEnabled()) {
      this.deadLetterTopicName = `${subscriptionName}-unack`
      this.deadLetterSubscriptionName = `${subscriptionName}-unack`

      this.deadLetterTopic = pubSubClient.topic(this.deadLetterTopicName)
      this.deadLetterSubscription = this.deadLetterTopic.subscription(this.deadLetterSubscriptionName)
    }
  }

  public async initialize() {
    try {
      await this.initializeTopic(this.topicName, this.topic)
      await this.initializeDeadLetterTopic()

      await this.initializeSubscription(this.subscriptionName, this.subscription, this.getInitializationOptions())
      await this.initializeDeadLetterSubscription()
    } catch (e) {
      this.logger?.error(`PubSub: Failed to initialize subscriber ${this.subscriptionName}`, e)
      process.abort()
    }
  }

  public start(asyncCallback: (msg: IParsedMessage<T>) => Promise<void>) {
    this.subscription.on('error', this.processError)
    this.subscription.on('message', this.processMsg(asyncCallback))

    this.logger?.info(`PubSub: Subscription ${this.subscriptionName} is started for topic ${this.topicName}`)
  }

  private logMessage(message: Message, dataParsed: T) {
    const messageInfo = {
      id: message.id,
      ackId: message.ackId,
      attributes: message.attributes,
      publishTime: message.publishTime?.toISOString(),
      received: message.received,
      deliveryAttempt: message.deliveryAttempt,
    }

    this.logger?.info(
      `PubSub: Got message on topic: ${this.topicName} with subscription: ${this.subscriptionName} with data:`,
      { messageInfo, dataParsed },
    )
  }

  private parseData(message: Message): T {
    const dataParser = new DataParser()
    const dataParsed = dataParser.parse(message.data) as T
    this.logMessage(message, dataParsed)
    return dataParsed
  }

  private processMsg(asyncCallback: (msg: IParsedMessage<T>) => Promise<void>) {
    return (message: Message) => {
      const dataParsed = this.parseData(message)
      const messageParsed = Object.assign(message, { dataParsed })
      asyncCallback(messageParsed).catch(e => {
        message.nack()
        this.logger?.error(`PubSub: Subscription: ${this.subscriptionName} Failed to process message:`, e)
      })
    }
  }

  private processError = (error: unknown) => {
    this.logger?.warn(`Subscription ${this.subscriptionName} failed with error`, error)

    // Commented to validate if it's still needed. In case of connection errors subscriber supposed to reconnect
    // automatically
    //
    // await this.subscription.close()
    // this.subscription.open()
    // this.logger?.info(`Reopened subscription ${this.subscriptionName} after error`, { error, })
  }

  private async initializeTopic(topicName: string, topic: Topic) {
    const [exist] = await topic.exists()
    this.logger?.info(`PubSub: Topic ${topicName} ${exist ? 'exists' : 'does not exist'}`)

    if (!exist) {
      await topic.create(createCallOptions)
      this.logger?.info(`PubSub: Topic ${topicName} is created`)
    }
  }

  private async initializeSubscription(
    subscriptionName: string,
    subscription: Subscription,
    options?: ISubscriptionInitializationOptions,
  ) {
    const [exist] = await subscription.exists()
    this.logger?.info(`PubSub: Subscription ${subscriptionName} ${exist ? 'exists' : 'does not exist'}`)

    if (!exist) {
      await subscription.create({ ...options, gaxOpts: createCallOptions })
      this.logger?.info(`PubSub: Subscription ${subscriptionName} is created`)
    } else if (options) {
      await subscription.setMetadata(options)
      this.logger?.info(`PubSub: Subscription ${subscriptionName} metadata updated`)
    }
  }

  private async initializeDeadLetterTopic() {
    if (this.deadLetterTopicName && this.deadLetterTopic) {
      await this.initializeTopic(this.deadLetterTopicName, this.deadLetterTopic)
      await this.addPubsubServiceAccountRole(this.deadLetterTopic.iam, 'roles/pubsub.publisher')
    }
  }

  private async initializeDeadLetterSubscription() {
    if (this.deadLetterSubscriptionName && this.deadLetterSubscription) {
      await this.initializeSubscription(this.deadLetterSubscriptionName, this.deadLetterSubscription)
      await this.addPubsubServiceAccountRole(this.subscription.iam, 'roles/pubsub.subscriber')
    }
  }

  private async addPubsubServiceAccountRole(iam: IAM, role: 'roles/pubsub.subscriber' | 'roles/pubsub.publisher') {
    const gcloudProjectId = this.subscriberOptions.subscriptionOptions?.gcloudProject?.id
    if (!gcloudProjectId) {
      this.logger?.error('Dead lettering enabled but no gcloud project id provided')
      return
    }

    const pubsubServiceAccount = `serviceAccount:service-${gcloudProjectId}@gcp-sa-pubsub.iam.gserviceaccount.com`
    await iam.setPolicy({
      bindings: [
        {
          members: [pubsubServiceAccount],
          role,
        },
      ],
    })
  }

  private isDeadLetterPolicyEnabled() {
    return Boolean(this.subscriberOptions?.subscriptionOptions?.maxDeliveryAttempts)
  }

  private getInitializationOptions(): ISubscriptionInitializationOptions {
    const options: ISubscriptionInitializationOptions = {
      deadLetterPolicy: null,
      retryPolicy: {},
    }

    const { subscriptionOptions } = this.subscriberOptions
    if (!subscriptionOptions) {
      return options
    }

    if (subscriptionOptions.minBackoffSeconds !== undefined) {
      options.retryPolicy.minimumBackoff = {
        seconds: subscriptionOptions.minBackoffSeconds,
      }
    }

    if (subscriptionOptions.maxBackoffSeconds !== undefined) {
      options.retryPolicy.maximumBackoff = {
        seconds: subscriptionOptions.maxBackoffSeconds,
      }
    }

    if (this.deadLetterTopic && this.deadLetterTopicName) {
      const gcloudProjectName = subscriptionOptions.gcloudProject?.name
      if (!gcloudProjectName) {
        this.logger?.error('Dead lettering enabled but no gcloud project name provided')
      } else {
        options.deadLetterPolicy = {
          maxDeliveryAttempts: subscriptionOptions.maxDeliveryAttempts,
          deadLetterTopic: `projects/${gcloudProjectName}/topics/${this.deadLetterTopicName}`,
        }
      }
    }

    return options
  }

  private getStartupOptions(options?: ISubscriptionOptions): SubscriptionOptions {
    return {
      ackDeadline: options?.ackDeadline,
      flowControl: {
        allowExcessMessages: options?.allowExcessMessages,
        maxMessages: options?.maxMessages,
      },
      streamingOptions: {
        maxStreams: options?.maxStreams,
      },
    }
  }
}
