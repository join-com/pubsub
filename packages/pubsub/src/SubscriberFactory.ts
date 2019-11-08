import { PubSub } from '@google-cloud/pubsub'
import { ITaskExecutor } from './DefaultTaskExecutor'
import { Options, Subscriber } from './Subscriber'

export type SubscriberInitializer<T> = (
  subscriptionName: string,
  options?: Options
) => Subscriber<T>

export class SubscriberFactory {
  constructor(
    readonly options?: Options,
    readonly client: PubSub = new PubSub(),
    readonly taskExecutor?: ITaskExecutor
  ) {}

  public getSubscription<T>(
    topicName: string,
    subscriptionName: string,
    options?: Options
  ): Subscriber<T> {
    return new Subscriber(
      topicName,
      subscriptionName,
      this.client,
      options || this.options,
      this.taskExecutor
    )
  }

  protected getSubscriberInitializer<T>(
    topicName: string
  ): SubscriberInitializer<T> {
    return (subscriptionName: string, options?: Options) => {
      return this.getSubscription(topicName, subscriptionName, options)
    }
  }
}
