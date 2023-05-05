import { PubSub, Topic } from '@google-cloud/pubsub'
import { Schema } from 'avsc'
import * as avro from 'avsc'

export class TopicHandler {

  protected readonly client: PubSub
  protected topic: Topic

  constructor(client: PubSub, topicName: string) {
    this.client = client
    this.topic = client.topic(topicName)
  }

  protected async getTopicType() {
    // const schemaName = topic.metadata?.schemaSettings?.schema
    const metadata = await this.topic.getMetadata()
    const schemaName = metadata?.[0].schemaSettings?.schema
    if (!schemaName) {
      return undefined
    }
    const topicSchema = await this.client.schema(schemaName).get()
    if (!topicSchema.definition) {
      return undefined
    }

    const schema = JSON.parse(topicSchema.definition) as Schema
    return avro.Type.forSchema(schema)
  }
}
