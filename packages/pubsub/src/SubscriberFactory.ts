import { PubSub } from '@google-cloud/pubsub'
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { ILogger } from './ILogger'
import { ISubscriptionOptions, Subscriber, IParsedMessage, ISubscriberOptions } from './Subscriber'

export interface ISubscriber<T> {
  topicName: string
  subscriptionName: string
  initialize: () => Promise<void>
  start: (asyncCallback: (msg: IParsedMessage<T>) => Promise<void>) => void
  stop: () => Promise<void>
}

export class SubscriberFactory<T> {
  private readonly client: PubSub
  private readonly schemaServiceClient: SchemaServiceClient

  constructor(private readonly defaultOptions: ISubscriptionOptions, private readonly logger: ILogger) {
    this.client = new PubSub()
    this.schemaServiceClient = new SchemaServiceClient()
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
    return new Subscriber(subscriberOptions, this.client, this.schemaServiceClient, this.logger)
  }
}
