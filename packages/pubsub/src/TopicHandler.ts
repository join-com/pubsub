import { PubSub, Topic } from '@google-cloud/pubsub'
import { Schema, Type } from 'avsc'
import { DateType } from './logical-types/DateType'

export interface ISchemaType {
  type: Type
  schemaRevisionId: string | null | undefined
}

export class TopicHandler {

  constructor(private readonly client: PubSub, private readonly topic: Topic) {
  }

  public async getTopicType(): Promise<ISchemaType | undefined> {
    const [metadata] = await this.topic.getMetadata()
    const schemaName = metadata?.schemaSettings?.schema
    if (!schemaName) {
      return undefined
    }
    return await this.getSchemaType(schemaName)
  }

  public async getSchemaType(schemaName: string): Promise<ISchemaType | undefined> {
    const topicSchema = await this.client.schema(schemaName).get()
    if (!topicSchema.definition) {
      return undefined
    }
    const schema = JSON.parse(topicSchema.definition) as Schema
    return {
      type: Type.forSchema(schema, { logicalTypes: { 'timestamp-micros': DateType } }),
      schemaRevisionId: topicSchema.revisionId
    }
  }
}
