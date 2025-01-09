import { IAM, Message, PubSub, Subscription, SubscriptionOptions, Topic } from '@google-cloud/pubsub'
import { google } from '@google-cloud/pubsub/build/protos/protos'
import { SchemaServiceClient, SubscriberClient } from '@google-cloud/pubsub/build/src/v1'
import { Type } from 'avsc'
import { createCallOptions } from './createCallOptions'
import { FieldsProcessor } from './FieldsProcessor'
import { ILogger } from './ILogger'
import { JOIN_PRESERVE_NULL, JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS } from './Publisher'
import { SchemaCache } from './SchemaCache'
import { replaceNullsWithUndefined } from './util'
import ISubscription = google.pubsub.v1.ISubscription

export interface IParsedMessage<T = unknown> {
  dataParsed: T
  ack: () => void
  nack: () => void
}

export interface IMessageInfo {
  id: string
  receivedAt: Date
}

/**
 * Subscription options
 * Filter is immutable and can't be changed after subscription is created
 */
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
  labels?: { [k: string]: string } | null
  filter?: string
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
  labels?: { [k: string]: string } | null
  filter?: string
}

export class Subscriber<T = unknown> {
  public readonly topicName: string
  public readonly subscriptionName: string

  private readonly topic: Topic
  private readonly subscription: Subscription

  private readonly deadLetterTopicName?: string
  private readonly deadLetterSubscriptionName?: string

  private readonly deadLetterTopic?: Topic
  private readonly deadLetterSubscription?: Subscription

  private readonly schemaCache: SchemaCache
  private readonly fieldsProcessor: FieldsProcessor

  constructor(
    private readonly subscriberOptions: ISubscriberOptions,
    pubSubClient: PubSub,
    private readonly schemaServiceClient: SchemaServiceClient,
    private readonly subscriberClient: SubscriberClient,
    private readonly logger?: ILogger,
  ) {
    const { topicName, subscriptionName, subscriptionOptions } = subscriberOptions

    this.topicName = topicName
    this.subscriptionName = subscriptionName

    this.topic = pubSubClient.topic(topicName)
    this.subscription = this.topic.subscription(subscriptionName, this.getStartupOptions(subscriptionOptions))
    this.schemaCache = new SchemaCache(this.schemaServiceClient, this.subscriberClient, this.topicName, this.logger)
    this.fieldsProcessor = new FieldsProcessor()

    if (this.isDeadLetterPolicyEnabled()) {
      this.deadLetterTopicName = `${subscriptionName}-unack`
      this.deadLetterSubscriptionName = `${subscriptionName}-unack`

      this.deadLetterTopic = pubSubClient.topic(this.deadLetterTopicName)
      this.deadLetterSubscription = this.deadLetterTopic.subscription(this.deadLetterSubscriptionName)
    }
  }

  public async initialize(): Promise<void> {
    try {
      if (process.env['PUBSUB_SUBSCRIPTION_TOPIC_INIT'] === 'true') {
        await this.initializeTopic(this.topicName, this.topic)
      }

      await this.initializeDeadLetterTopic()

      await this.initializeSubscription(this.subscriptionName, this.subscription, this.getInitializationOptions())
      await this.initializeDeadLetterSubscription()
    } catch (e) {
      this.logger?.error(`PubSub: Failed to initialize subscriber ${this.subscriptionName}`, e)
      process.abort()
    }
  }

  public start(asyncCallback: (msg: IParsedMessage<T>, info: IMessageInfo) => Promise<void>): void {
    this.subscription.on('error', this.processError)
    this.subscription.on('message', this.processMsg(asyncCallback))

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

  private async parseData(message: Message): Promise<T> {
    let schemaId = message.attributes['googclient_schemarevisionid']
    if (!schemaId) {
      this.logger?.error(`PubSub: Subscription: ${this.subscriptionName} Message does not have schema revision id`, {
        message,
      })
      schemaId = await this.schemaCache.getLatestSchemaRevisionId()
    }

    const dataParsed = await this.parseAvroMessage(message, schemaId)
    const undefinedOrNullOptionalArrays = message.attributes[JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS]
    if (undefinedOrNullOptionalArrays) {
      this.fieldsProcessor.setEmptyArrayFieldsToUndefined(
        dataParsed as Record<string, unknown>,
        undefinedOrNullOptionalArrays.split(','),
      )
    }
    replaceNullsWithUndefined(dataParsed, message.attributes[JOIN_PRESERVE_NULL])
    this.logMessage(message, dataParsed)
    return dataParsed
  }

  private async parseAvroMessage(message: Message, schemaRevisionId: string): Promise<T> {
    const type: Type = await this.schemaCache.getTypeFromCacheOrRemote(schemaRevisionId)
    return type.fromString(message.data.toString()) as T
  }

  private processMsg(
    asyncCallback: (msg: IParsedMessage<T>, info: IMessageInfo) => Promise<void>,
  ): (message: Message) => void {
    const asyncMessageProcessor = async (message: Message) => {
      const dataParsed = await this.parseData(message).catch(e => {
        this.logger?.error(`PubSub: Subscription: ${this.subscriptionName} Failed to parse message:`, e)
        return undefined
      })

      // If message parsing failed, nack the message and skip processing
      if (!dataParsed) {
        message.nack()
        return
      }

      const messageParsed = Object.assign(message, { dataParsed })
      const info: IMessageInfo = {
        id: message.id,
        receivedAt: new Date(message.received),
      }
      asyncCallback(messageParsed, info).catch(e => {
        message.nack()
        this.logger?.error(`PubSub: Subscription: ${this.subscriptionName} Failed to process message:`, e)
      })
    }

    return (message: Message) => {
      asyncMessageProcessor(message)
        .then(_ => {
          return
        })
        .catch(e => {
          throw e
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
      const [existingSubscription] = await subscription.getMetadata()
      if ((options.filter || existingSubscription.filter) && options.filter != existingSubscription.filter) {
        throw new Error(
          `PubSub: Subscriptions filters are immutable, they can't be changed, subscription: ${subscriptionName},` +
            ` currentFilter: ${existingSubscription.filter as string}, newFilter: ${options.filter as string}`,
        )
      }
      if (this.isMetadataChanged(existingSubscription, options)) {
        await subscription.setMetadata({
          retryPolicy: options.retryPolicy,
          deadLetterPolicy: options.deadLetterPolicy,
          labels: options?.labels
        })
        this.logger?.info(`PubSub: Subscription ${subscriptionName} metadata updated`)
      }
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

    if (subscriptionOptions.labels !== undefined) {
      options.labels = subscriptionOptions.labels
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

    if (subscriptionOptions.filter) {
      options.filter = subscriptionOptions.filter
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

  private isMetadataChanged(existingSubscription: ISubscription, options: ISubscriptionInitializationOptions): boolean {
    if (
      options.retryPolicy.minimumBackoff?.seconds &&
      String(options.retryPolicy.minimumBackoff.seconds) !== existingSubscription.retryPolicy?.minimumBackoff?.seconds
    ) {
      return true
    }
    if (
      options.retryPolicy.maximumBackoff?.seconds &&
      String(options.retryPolicy.maximumBackoff.seconds) !== existingSubscription.retryPolicy?.maximumBackoff?.seconds
    ) {
      return true
    }
    if (
      !!options.deadLetterPolicy?.maxDeliveryAttempts &&
      options.deadLetterPolicy.maxDeliveryAttempts !== existingSubscription.deadLetterPolicy?.maxDeliveryAttempts
    ) {
      return true
    }
    if (
      (!!options.labels && JSON.stringify(existingSubscription.labels) !== JSON.stringify(options.labels)) ||
      (options.labels == null && !!existingSubscription.labels && Object.keys(existingSubscription.labels).length !== 0)
    ) {
      return true
    }

    return false
  }
}
