import { PubSub } from '@google-cloud/pubsub'
import { ILogger } from './ILogger'
import { Publisher } from './Publisher'

export interface IPublisher<R extends string, T> {
  topicName: R
  initialize: () => Promise<void>
  publishMsg: (data: T) => Promise<void>
  flush: () => Promise<void>
}

export class PublisherFactory<T> {
  private readonly client: PubSub

  constructor(private readonly logger: ILogger) {
    this.client = new PubSub()
  }

  public getPublisher<R extends string, K extends keyof T> (topic: R, avroSchemas?: Record<R, { writer: object, reader: object }>): IPublisher<R, T[K]> {
    return new Publisher(topic, this.client, this.logger, avroSchemas)
  }
}
