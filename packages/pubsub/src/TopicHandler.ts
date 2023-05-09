import { PubSub, Topic } from '@google-cloud/pubsub'
import { Schema, Type } from 'avsc'
import { DateType } from './logical-types/DateType'

export class TopicHandler {

  protected readonly client: PubSub
  protected topic: Topic

  constructor(client: PubSub, topicName: string) {
    this.client = client
    this.topic = client.topic(topicName)
  }

  protected async getTopicType(): Promise<Type | undefined> {
    const [metadata] = await this.topic.getMetadata()
    const schemaName = metadata?.schemaSettings?.schema
    if (!schemaName) {
      return undefined
    }
    const topicSchema = await this.client.schema(schemaName).get()
    if (!topicSchema.definition) {
      return undefined
    }

    const schema = JSON.parse(topicSchema.definition) as Schema
    return Type.forSchema(schema,  {logicalTypes: {'timestamp-micros': DateType}})
  }
}
