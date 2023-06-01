import { IAM, Message, PubSub, Subscription, SubscriptionOptions, Topic } from '@google-cloud/pubsub'
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { env } from '@join-com/process-env'
import { Type } from 'avsc'
import { createCallOptions } from './createCallOptions'
import { DataParser } from './DataParser'
import { ILogger } from './ILogger'
import { DateType } from './logical-types/DateType'
import { replaceNullsWithUndefined } from './util'

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
  readonly topicSchemaName: string

  private readonly topic: Topic
  private readonly subscription: Subscription

  private readonly deadLetterTopicName?: string
  private readonly deadLetterSubscriptionName?: string

  private readonly deadLetterTopic?: Topic
  private readonly deadLetterSubscription?: Subscription

  private readonly topicTypesCache: Record<string, Type> = {}

  constructor(
    private readonly subscriberOptions: ISubscriberOptions,
    pubSubClient: PubSub,
    private readonly schemaServiceClient: SchemaServiceClient,
    private readonly logger?: ILogger,
  ) {
    const { topicName, subscriptionName, subscriptionOptions } = subscriberOptions

    this.topicName = topicName
    this.subscriptionName = subscriptionName
    this.topicSchemaName = `${this.topicName}-generated-avro`

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

  public async stop(): Promise<void> {
    this.logger?.info(`PubSub: Closing subscription ${this.subscriptionName}`)
    await this.subscription.close()
  }

  private logMessage(message: Message, dataParsed: T, schemaRevisionId?: string) {
    const messageInfo = {
      id: message.id,
      ackId: message.ackId,
      attributes: message.attributes,
      publishTime: message.publishTime?.toISOString(),
      received: message.received,
      deliveryAttempt: message.deliveryAttempt,
      schemaRevisionId: schemaRevisionId
    }

    this.logger?.info(
      `PubSub: Got message on topic: ${this.topicName} with subscription: ${this.subscriptionName} with data:`,
      { messageInfo, dataParsed },
    )
  }

  private async parseData(message: Message): Promise<T> {
    let dataParsed: T
    const schemaId = message.attributes['googclient_schemarevisionid']
    // TODO: remove if else block as only avro should be used, throw error if there is no schema revision
    if (schemaId) {
      dataParsed = await this.parseAvroMessage(message, schemaId)
      replaceNullsWithUndefined(dataParsed)
    } else {
      const dataParser = new DataParser()
      dataParsed = dataParser.parse(message.data) as T
    }
    this.logMessage(message, dataParsed)
    return dataParsed
  }

  private async parseAvroMessage(message: Message, schemaId: string): Promise<T> {
    const type: Type = await this.getTypeFromCacheOrRemote(schemaId)
    return type.fromString(message.data.toString()) as T
  }

  private async getTypeFromCacheOrRemote(schemaRevisionId: string): Promise<Type> {
    const typeFromCache = this.topicTypesCache[schemaRevisionId]
    if (typeFromCache) {
      return typeFromCache
    }
    const revision = `projects/${env('GCLOUD_PROJECT').asString()}/schemas/${this.topicSchemaName}@${schemaRevisionId}`
    const [schema] = await this.schemaServiceClient.getSchema({ name: revision })
    // schema must always have a definition, so we want to fail if there is a wrong schema without a definition
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    return Type.forSchema(schema.definition!, { logicalTypes: { 'timestamp-micros': DateType } })
  }

  private processMsg(asyncCallback: (msg: IParsedMessage<T>) => Promise<void>): (message: Message) => void {
    const asyncMessageProcessor = async (message: Message) => {
      const dataParsed = await this.parseData(message)
      const messageParsed = Object.assign(message, { dataParsed })
      asyncCallback(messageParsed).catch(e => {
        message.nack()
        this.logger?.error(`PubSub: Subscription: ${this.subscriptionName} Failed to process message:`, e)
      })
    }

    return (message: Message) => {
      asyncMessageProcessor(message).then(_ => {
        return
      }).catch(e => {
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
