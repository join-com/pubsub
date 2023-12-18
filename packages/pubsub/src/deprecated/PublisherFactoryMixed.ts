import { PubSub } from '@google-cloud/pubsub'
import { ILogger } from '../ILogger'
import { IPublisher } from '../PublisherFactory'
import { PublisherMixed } from './PublisherMixed'

/**
 * @deprecated should be used only when migration between json/avro is used
 */
export class PublisherFactoryMixed<TypeMap> {
  private readonly client: PubSub

  constructor(private readonly logger: ILogger, private readonly avroSchemas: Record<keyof TypeMap, { writer: object, reader: object }>) {
    this.client = new PubSub()
  }

  public getPublisher<Topic extends keyof TypeMap> (topic: Topic): IPublisher<TypeMap[Topic]> {
    return new PublisherMixed(topic.toString(), this.client, this.logger, this.avroSchemas[topic])
  }
}

/**
 * @deprecated should be used only when migration of the events/commands is not possible
 */
export class PublisherFactorySchemaless<TypeMap> {
  private readonly client: PubSub

  constructor(private readonly logger: ILogger, private readonly avroSchemas?: Record<keyof TypeMap, { writer: object, reader: object }>) {
    this.client = new PubSub()
  }

  public getPublisher<Topic extends keyof TypeMap> (topic: Topic): IPublisher<TypeMap[Topic]> {
    return new PublisherMixed(topic.toString(), this.client, this.logger, this.avroSchemas?.[topic])
  }
}
