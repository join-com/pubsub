import { PubSub } from '@google-cloud/pubsub'
import { Subscriber, Options } from './Subscriber'

export type SubscriberInitializer<T> = (
  subscriptionName: string,
  options?: Options
) => Subscriber<T>

export class SubscriberFactory {
  constructor(protected client: PubSub = new PubSub()) {}

  public getSubscription<T>(
    topicName: string,
    subscriptionName: string,
    options?: Options
  ): Subscriber<T> {
    return new Subscriber(topicName, subscriptionName, this.client, options)
  }

  protected getSubscriberInitializer<T>(
    topicName: string
  ): SubscriberInitializer<T> {
    return (subscriptionName: string, options?: Options) => {
      return this.getSubscription(topicName, subscriptionName, options)
    }
  }
}
