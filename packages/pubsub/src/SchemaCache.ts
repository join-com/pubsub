import { google } from '@google-cloud/pubsub/build/protos/protos'
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { Schema, Type } from 'avsc'
import { DateType } from './logical-types/DateType'
import SchemaView = google.pubsub.v1.SchemaView

export class SchemaCache {

  private readonly topicTypeRevisionsCache: Record<string, Type> = {}

  constructor(
    private readonly schemaServiceClient: SchemaServiceClient,
    private readonly topicSchemaName: string
  ) { }

  public async getTypeFromCacheOrRemote(schemaRevisionId: string): Promise<Type> {
    const typeFromCache = this.topicTypeRevisionsCache[schemaRevisionId]
    if (typeFromCache) {
      return typeFromCache
    }
    const projectName = process.env['GCLOUD_PROJECT']
    if (!projectName) {
      throw new Error('Can\'t find GCLOUD_PROJECT env variable, please define it')
    }
    const revisionPath = `projects/${projectName}/schemas/${this.topicSchemaName}@${schemaRevisionId}`
    const [remoteSchema] = await this.schemaServiceClient.getSchema({ name: revisionPath })

    if (!remoteSchema.definition) {
      throw new Error(`Can't process schema ${schemaRevisionId} without definition`)
    }
    const schema = JSON.parse(remoteSchema.definition) as Schema
    const type = Type.forSchema(schema, { logicalTypes: { 'timestamp-micros': DateType } })
    this.topicTypeRevisionsCache[schemaRevisionId] = type

    return type
  }

  public async getLatestSchemaRevisionId(): Promise<string> {
    const projectName = process.env['GCLOUD_PROJECT']
    if (!projectName) {
      throw new Error('Can\'t find GCLOUD_PROJECT env variable, please define it')
    }
    const schemaPath = `projects/${projectName}/schemas/${this.topicSchemaName}`

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
}
