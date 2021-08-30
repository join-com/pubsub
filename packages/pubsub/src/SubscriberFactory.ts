import { PubSub } from '@google-cloud/pubsub'
import { ISubscriptionOptions, Subscriber } from './Subscriber'

export class SubscriberFactory<T> {
  private readonly client: PubSub

  constructor(private readonly defaultOptions: ISubscriptionOptions) {
    this.client = new PubSub()
  }

  public getSubscriber<K extends keyof T>(
    topic: K,
    subscription: string,
    options?: ISubscriptionOptions,
  ): Subscriber<T[K]> {
    return new Subscriber(topic.toString(), subscription, this.client, options ?? this.defaultOptions)
  }
}
