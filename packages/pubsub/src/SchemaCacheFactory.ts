import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { SchemaCache } from './SchemaCache'

export class SchemaCacheFactory {
  private readonly schemaServiceClient: SchemaServiceClient

  constructor() {
    this.schemaServiceClient = new SchemaServiceClient()
  }

  public getSchemaCache(topicSchemaName: string): SchemaCache {
    return new SchemaCache(this.schemaServiceClient, topicSchemaName)
  }
}
