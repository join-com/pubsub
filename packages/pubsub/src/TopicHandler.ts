import { PubSub, Topic } from '@google-cloud/pubsub'
import { Schema, Type } from 'avsc'
import { ILogger } from './ILogger'
import { DateType } from './logical-types/DateType'

export interface ISchemaType {
  type: Type
  schemaRevisionId: string | null | undefined
}

export class TopicHandler {

  constructor(private readonly client: PubSub, private readonly topic: Topic, private readonly logger?: ILogger) {
  }

  public async getTopicType(): Promise<ISchemaType | undefined> {
    const [metadata] = await this.topic.getMetadata()
    const schemaName = metadata?.schemaSettings?.schema
    if (!schemaName) {
      this.logger?.info(`Couldn't find schema for topic: ${this.topic.name}`)
      return undefined
    }
    return await this.getSchemaType(schemaName)
  }

  public async getSchemaType(schemaName: string): Promise<ISchemaType | undefined> {
    const topicSchema = await this.client.schema(schemaName).get()
    if (!topicSchema.definition) {
      this.logger?.info(`Couldn't find schema with name: ${schemaName}`)
      return undefined
    }
    const schema = JSON.parse(topicSchema.definition) as Schema
    this.logger?.info(`For schema ${schemaName} next revisionId is used: ${topicSchema?.revisionId || 'no-revision-in-registry'}`)
    return {
      type: Type.forSchema(schema, { logicalTypes: { 'timestamp-micros': DateType } }),
      schemaRevisionId: topicSchema.revisionId
    }
  }
}
