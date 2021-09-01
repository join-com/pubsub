import { PubSub } from '@google-cloud/pubsub'
import { Publisher } from './Publisher'

export interface IPublisher<T> {
  topicName: string
  initialize: () => Promise<void>
  publishMsg: (data: T) => Promise<void>
}

export class PublisherFactory<T> {
  private readonly client: PubSub

  constructor() {
    this.client = new PubSub()
  }

  public getPublisher<K extends keyof T>(topic: K): IPublisher<T[K]> {
    return new Publisher(topic.toString(), this.client)
  }
}
