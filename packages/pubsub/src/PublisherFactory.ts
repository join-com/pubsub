import { PubSub } from '@google-cloud/pubsub'
import { Publisher } from './Publisher'

export class PublisherFactory<T> {
  private readonly client: PubSub

  constructor() {
    this.client = new PubSub()
  }

  public getPublisher<K extends keyof T>(topic: K): Publisher<T[K]> {
    return new Publisher(topic.toString(), this.client)
  }
}
