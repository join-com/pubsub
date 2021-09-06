import { PubSub } from '@google-cloud/pubsub'
import { ISubscriptionOptions, Subscriber, IParsedMessage } from './Subscriber'

export interface ISubscriber<T> {
  topicName: string
  subscriptionName: string
  initialize: () => Promise<void>
  start: (asyncCallback: (msg: IParsedMessage<T>) => Promise<void>) => void
}

export class SubscriberFactory<T> {
  private readonly client: PubSub

  constructor(private readonly defaultOptions: ISubscriptionOptions) {
    this.client = new PubSub()
  }

  public getSubscriber<K extends keyof T>(
    topic: K,
    subscription: string,
    options?: ISubscriptionOptions
  ): ISubscriber<T[K]> {
    return new Subscriber(
      topic.toString(),
      subscription,
      this.client,
      options ?? this.defaultOptions
    )
  }
}
