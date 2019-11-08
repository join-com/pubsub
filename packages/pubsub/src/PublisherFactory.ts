import { PubSub } from '@google-cloud/pubsub'
import { Publisher } from './Publisher'

export class PublisherFactory {
  constructor(private client: PubSub = new PubSub()) {}

  public getPublisher<T>(topicName: string): Publisher<T> {
    return new Publisher(topicName, this.client)
  }
}
