import { PubSub } from '@google-cloud/pubsub'
import { ILogger } from './ILogger'
import { ISubscriptionOptions, Subscriber, IParsedMessage, ISubscriberOptions } from './Subscriber'

export interface ISubscriber<T> {
  topicName: string
  subscriptionName: string
  initialize: () => Promise<void>
  start: (asyncCallback: (msg: IParsedMessage<T>) => Promise<void>) => void
}

export class SubscriberFactory<T> {
  private readonly client: PubSub

  constructor(private readonly defaultOptions: ISubscriptionOptions, private readonly logger?: ILogger) {
    this.client = new PubSub()
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
    return new Subscriber(subscriberOptions, this.client, this.logger)
  }
}
