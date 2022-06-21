import { PubSub } from '@google-cloud/pubsub'
import { ILogger, Publisher } from '@join-com/pubsub'

export class PublisherFactory {
  private readonly client: PubSub

  constructor(private readonly logger?: ILogger) {
    this.client = new PubSub()
  }

  public getPublisher<T>(topicName: string): Publisher<T> {
    return new Publisher(topicName, this.client, this.logger)
  }
}
