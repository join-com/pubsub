import { PubSub } from '@google-cloud/pubsub'
import { ILogger } from './ILogger'
import { Publisher } from './Publisher'

export interface IPublisher<T> {
  topicName: string
  initialize: () => Promise<void>
  publishMsg: (data: T) => Promise<void>
  flush: () => Promise<void>
}

export class PublisherFactory<TypeMap> {
  private readonly client: PubSub

  constructor(private readonly logger: ILogger, private readonly avroSchemas: Record<keyof TypeMap, { writer: object, reader: object }>) {
    this.client = new PubSub()
  }

  public getPublisher<Topic extends keyof TypeMap> (topic: Topic): IPublisher<TypeMap[Topic]> {
    return new Publisher(topic.toString(), this.client, this.avroSchemas[topic], this.logger)
  }
}
