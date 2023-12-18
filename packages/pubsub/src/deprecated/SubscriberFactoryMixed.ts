import { PubSub } from '@google-cloud/pubsub'
import { SchemaServiceClient, SubscriberClient } from '@google-cloud/pubsub/build/src/v1'
import { ILogger } from '../ILogger'
import { ISubscriptionOptions, Subscriber, ISubscriberOptions } from '../Subscriber'
import { ISubscriber, PUBSUB_CLIENT_CONNECTION_LIMIT, PUBSUB_DEFAULT_MAX_STREAMS } from '../SubscriberFactory'

/**
 * @deprecated should be used only when migration between json/avro is used
 */
export class SubscriberFactoryMixed<T> {
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
