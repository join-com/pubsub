import { google } from '@google-cloud/pubsub/build/protos/protos'
import { SchemaServiceClient, SubscriberClient } from '@google-cloud/pubsub/build/src/v1'
import { Schema, Type } from 'avsc'
import { ILogger } from './ILogger'
import { DateType } from './logical-types/DateType'
import SchemaView = google.pubsub.v1.SchemaView

const UNACK_SUFFIX = '-unack'

export class SchemaCache {

  private readonly topicTypeRevisionsCache: Record<string, Type> = {}
  private topicSchemaName: string | undefined
  private projectName: string

  constructor(
    private readonly schemaServiceClient: SchemaServiceClient,
    private readonly subscriberClient: SubscriberClient,
    private readonly topicName: string,
    private readonly logger?: ILogger
  ) {
    this.projectName = process.env['GCLOUD_PROJECT'] as string
    if (!this.projectName) {
      throw new Error('Can\'t find GCLOUD_PROJECT env variable, please define it')
    }
  }

  public async getTypeFromCacheOrRemote(schemaRevisionId: string): Promise<Type> {
    if (!this.topicSchemaName) {
      this.topicSchemaName = await this.getTopicSchemaName()
    }
    const typeFromCache = this.topicTypeRevisionsCache[schemaRevisionId]
    if (typeFromCache) {
      return typeFromCache
    }

    const revisionPath = `projects/${this.projectName}/schemas/${this.topicSchemaName}@${schemaRevisionId}`
    let remoteSchema
    try {
      [remoteSchema] = await this.schemaServiceClient.getSchema({ name: revisionPath })
    } catch (error) {
      if (error instanceof Error && error.message.includes('NOT_FOUND')) {
        this.logger?.warn(`Can't get schema for revision: ${schemaRevisionId}, trying to get the latest one`, error)
        const latestSchemaId = await this.getLatestSchemaRevisionId()
        const type = this.topicTypeRevisionsCache[latestSchemaId]
        if (type) {
          return type
        } else {
          throw error
        }
      } else {
        throw error
      }
    }

    if (!remoteSchema.definition) {
      throw new Error(`Can't process schema ${schemaRevisionId} without definition`)
    }
    const schema = JSON.parse(remoteSchema.definition) as Schema
    const type = Type.forSchema(schema, { logicalTypes: { 'timestamp-micros': DateType } })
    this.topicTypeRevisionsCache[schemaRevisionId] = type

    return type
  }

  public async getLatestSchemaRevisionId(): Promise<string> {
    if (!this.topicSchemaName) {
      this.topicSchemaName = await this.getTopicSchemaName()
    }
    const schemaPath = `projects/${this.projectName}/schemas/${this.topicSchemaName}`
    const revisionsResponse = await this.schemaServiceClient.listSchemaRevisions({
      name: schemaPath,
      pageSize: 1,
      view: SchemaView.FULL
    })
    if (revisionsResponse[0].length == 0 || !revisionsResponse[0][0]) {
      throw Error(`Can'\t find any schemas for the topic ${schemaPath}`)
    }
    const remoteSchema = revisionsResponse[0][0]
    const schemaRevisionId = remoteSchema.revisionId
    if (!schemaRevisionId) {
      throw new Error(`Can't process schema ${schemaPath} without revisionId`)
    }
    if (!remoteSchema.definition) {
      throw new Error(`Can't process schema ${schemaPath}/${schemaRevisionId} without definition`)
    }
    const schema = JSON.parse(remoteSchema.definition) as Schema
    this.topicTypeRevisionsCache[schemaRevisionId] = Type.forSchema(schema, { logicalTypes: { 'timestamp-micros': DateType } })

    return schemaRevisionId
  }

  private async getTopicSchemaName(): Promise<string> {
    if (!this.topicName.endsWith(UNACK_SUFFIX)) {
      return `${this.topicName}-generated-avro`
    }
    const originalSubscriptionName = this.topicName.substring(0, this.topicName?.lastIndexOf('-unack'))
    const subscriptionPath = `projects/${this.projectName}/subscriptions/${originalSubscriptionName}`

    const subscriptionResponse = await this.subscriberClient.getSubscription({subscription: subscriptionPath})
    const originalSubscription = subscriptionResponse[0]
    if (!originalSubscription.topic) {
      throw new Error('Can\'t find unacked subscription original topic name')
    }
    const topicPathParts = originalSubscription.topic.split('/')
    const topicName = topicPathParts[topicPathParts.length - 1] as string
    return `${topicName}-generated-avro`
  }
}
