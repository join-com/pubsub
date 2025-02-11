import { PubSub } from '@google-cloud/pubsub'
import { SchemaServiceClient, SubscriberClient } from '@google-cloud/pubsub/build/src/v1'
import { ILogger } from './ILogger'
import { ISubscriptionOptions, Subscriber, IParsedMessage, ISubscriberOptions, IMessageInfo } from './Subscriber'

/**
 * The PubSub client has a hard limit of 100 connections. However we already experience the issue with messages
 * acknowledgement with 20 subscribers x 5 streams per subscriber = 100 connections. So lowering this limit to 80.
 * https://github.com/googleapis/nodejs-pubsub/issues/1705
 */
const PUBSUB_CLIENT_CONNECTION_LIMIT = 80
const PUBSUB_DEFAULT_MAX_STREAMS = 5

export interface ISubscriber<T> {
  topicName: string
  subscriptionName: string
  initialize: () => Promise<void>
  start: (asyncCallback: (msg: IParsedMessage<T>, info: IMessageInfo) => Promise<void>) => void
  stop: () => Promise<void>
  logger?: ILogger
}

export class SubscriberFactory<T> {
  private readonly schemaServiceClient: SchemaServiceClient
  private readonly subscriberClient: SubscriberClient
  private currentClientConnections: number
  private currentClient: PubSub

  constructor(private readonly defaultOptions: ISubscriptionOptions, private readonly logger: ILogger) {
    this.currentClient = new PubSub()
    this.currentClientConnections = 0
    this.schemaServiceClient = new SchemaServiceClient()
    this.subscriberClient = new SubscriberClient()
  }

  public getSubscriber<K extends keyof T>(
    topicName: K,
    subscriptionName: string,
    options?: ISubscriptionOptions,
  ): ISubscriber<T[K]> {
    const subscriberOptions: ISubscriberOptions = {
      topicName: topicName.toString(),
      subscriptionName,
      subscriptionOptions: options ?? this.defaultOptions,
    }

    const requestedConnections = subscriberOptions.subscriptionOptions?.maxStreams ?? PUBSUB_DEFAULT_MAX_STREAMS
    if (this.currentClientConnections + requestedConnections <= PUBSUB_CLIENT_CONNECTION_LIMIT) {
      this.currentClientConnections += requestedConnections
    } else {
      this.currentClientConnections = requestedConnections
      this.currentClient = new PubSub()
      this.logger.info('SubscriberFactory: Reached PubSub client connections limit. Creating new client.')
    }

    return new Subscriber(subscriberOptions, this.currentClient, this.schemaServiceClient, this.subscriberClient, this.logger)
  }
}
