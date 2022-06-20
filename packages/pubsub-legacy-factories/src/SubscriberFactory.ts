import { PubSub } from '@google-cloud/pubsub'
import { ISubscriptionOptions, Subscriber } from '@join-com/pubsub'

type SubscriberInitializer<T> = (subscriptionName: string, options?: ISubscriptionOptions) => Subscriber<T>

export class SubscriberFactory {
  constructor(readonly options?: ISubscriptionOptions, readonly client: PubSub = new PubSub()) {}

  public getSubscription<T>(
    topicName: string,
    subscriptionName: string,
    options?: ISubscriptionOptions,
  ): Subscriber<T> {
    return new Subscriber({ topicName, subscriptionName, subscriptionOptions: options || this.options }, this.client)
  }

  protected getSubscriberInitializer<T>(topicName: string): SubscriberInitializer<T> {
    return (subscriptionName: string, options?: ISubscriptionOptions) => {
      return this.getSubscription(topicName, subscriptionName, options)
    }
  }
}
