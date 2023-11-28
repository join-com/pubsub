import { PubSub } from '@google-cloud/pubsub'
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { ISubscriptionOptions, Subscriber, ILogger } from '@join-com/pubsub'

export type SubscriberInitializer<T> = (subscriptionName: string, options?: ISubscriptionOptions) => Subscriber<T>

export class SubscriberFactory {
  private readonly client: PubSub
  private readonly schemaServiceClient: SchemaServiceClient

  constructor(readonly options: ISubscriptionOptions, private readonly logger: ILogger) {
    this.client = new PubSub()
    this.schemaServiceClient = new SchemaServiceClient()
  }

  public getSubscription<T>(
    topicName: string,
    subscriptionName: string,
    options?: ISubscriptionOptions,
  ): Subscriber<T> {
    return new Subscriber(
      { topicName, subscriptionName, subscriptionOptions: options || this.options },
      this.client,
      this.schemaServiceClient,
      this.logger,
    )
  }

  protected getSubscriberInitializer<T>(topicName: string): SubscriberInitializer<T> {
    return (subscriptionName: string, options?: ISubscriptionOptions) => {
      return this.getSubscription(topicName, subscriptionName, options)
    }
  }
}
