import { PubSub, Topic } from '@google-cloud/pubsub'
import { Schema, Type } from 'avsc'
import { ILogger } from './ILogger'
import { DateType } from './logical-types/DateType'

export interface ISchemaType {
  schemaRevisionId: string
  type: Type
}

export class TopicHandler {

  constructor(private readonly client: PubSub, private readonly topic: Topic, private readonly logger?: ILogger) {
  }

  public async getSchemaTypeFromTopic(): Promise<ISchemaType | undefined> {
    const [metadata] = await this.topic.getMetadata()
    const schemaName = metadata?.schemaSettings?.schema
    if (!schemaName) {
      this.logger?.info(`Couldn't find schema for topic: ${this.topic.name}`)
      return undefined
    }
    return await this.getSchemaType(schemaName)
  }

  public async getSchemaType(schemaNameOrRevisionId: string): Promise<ISchemaType> {
    const topicSchema = await this.client.schema(schemaNameOrRevisionId).get()
    if (!topicSchema || !topicSchema?.definition || !topicSchema?.revisionId) {
      throw new Error(`Couldn't find schema with name: ${schemaNameOrRevisionId}`)
    }
    const schema = JSON.parse(topicSchema.definition) as Schema
    this.logger?.info(`For schema ${schemaNameOrRevisionId} next revisionId is used: ${topicSchema.revisionId}`)
    return {
      type: Type.forSchema(schema, { logicalTypes: { 'timestamp-micros': DateType } }),
      schemaRevisionId: topicSchema.revisionId
    }
  }
}
