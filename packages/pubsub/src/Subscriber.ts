import * as util from 'util'
import { IAM, Message, PubSub, Subscription, SubscriptionOptions, Topic } from '@google-cloud/pubsub'
import { Type } from 'avsc'
import { createCallOptions } from './createCallOptions'
import { DataParser } from './DataParser'
import { ILogger } from './ILogger'
import { TopicHandler } from './TopicHandler'

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

export class Subscriber<T = unknown> extends TopicHandler {
  readonly topicName: string
  readonly subscriptionName: string

  private readonly subscription: Subscription

  private readonly deadLetterTopicName?: string
  private avroType: Type | undefined

  constructor(
    private readonly subscriberOptions: ISubscriberOptions,
    pubSubClient: PubSub,
    private readonly logger?: ILogger,
  ) {
    super(pubSubClient, subscriberOptions.topicName)
    const { topicName, subscriptionName, subscriptionOptions } = subscriberOptions

    this.topicName = topicName
    this.subscriptionName = subscriptionName

    this.subscription = this.topic.subscription(subscriptionName, this.getStartupOptions(subscriptionOptions))

    if (this.isDeadLetterPolicyEnabled()) {
      this.deadLetterTopicName = `${subscriptionName}-unack`
      this.deadLetterSubscriptionName = `${subscriptionName}-unack`

      this.deadLetterTopic = pubSubClient.topic(this.deadLetterTopicName)
      this.deadLetterSubscription = this.deadLetterTopic.subscription(this.deadLetterSubscriptionName)
    }
  }

  private readonly deadLetterSubscriptionName?: string
  private readonly deadLetterTopic?: Topic

  private readonly deadLetterSubscription?: Subscription

  public async initialize() {
    try {
      await this.initializeTopic(this.topicName, this.topic)

      await this.initializeDeadLetterTopic()

      await this.initializeSubscription(this.subscriptionName, this.subscription, this.getInitializationOptions())
      await this.initializeDeadLetterSubscription()

      this.avroType = await this.getTopicType()
    } catch (e) {
      this.logger?.error(`PubSub: Failed to initialize subscriber ${this.subscriptionName}`, e)
      process.abort()
    }
  }

  public start(asyncCallback: (msg: IParsedMessage<T>) => Promise<void>) {
    // subscription.on doesn't support async, so we need this wrapper
    // to run async in function returned by this.processMsg

    const subscriberCallback = (message: Message) => {
      util.callbackify(async (callbackifyMessage: Message) => {
        return await this.processMsg(asyncCallback)(callbackifyMessage)
      })(message, (_: object) =>{return});
    }

    this.subscription.on('error', this.processError)
    this.subscription.on('message', subscriberCallback)

    this.logger?.info(`PubSub: Subscription ${this.subscriptionName} is started for topic ${this.topicName}`)
  }

  public async stop(): Promise<void> {
    this.logger?.info(`PubSub: Closing subscription ${this.subscriptionName}`)
    await this.subscription.close()
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
    let dataParsed: T
    const dataParser = new DataParser()
    if (this.avroType) {
      const dataParsedWithNulls = this.avroType.fromBuffer(message.data) as T
      dataParsed = dataParser.replaceNullsWithUndefined(dataParsedWithNulls)
    } else {
      dataParsed = dataParser.parse(message.data) as T
    }
    this.logMessage(message, dataParsed)
    return dataParsed
  }

  private processMsg(asyncCallback: (msg: IParsedMessage<T>) => Promise<void>) {
    return async (message: Message) => {
      let dataParsed
      //TODO: try catch should be removed after all topics will start using avro
      try {
        dataParsed = this.parseData(message)
      } catch (e) {
        this.logger?.error(`Couldn't parse message, messageId: ${message.id}`)
        //if there is a schema, throw error, as we can't fix it, otherwise try to load schema and parse again
        if (this.avroType) {
         throw e
        }
        this.topic = this.client.topic(this.topicName)
        this.avroType = await this.getTopicType()
        dataParsed = this.parseData(message)
      }
      const messageParsed = Object.assign(message, { dataParsed })
      asyncCallback(messageParsed).catch(e => {
        message.nack()
        this.logger?.error(`PubSub: Subscription: ${this.subscriptionName} Failed to process message:`, e)
      })
    }
  }

  private processError = (error: unknown) => {
    this.logger?.warn(`Subscription ${this.subscriptionName} failed with error`, error)
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
