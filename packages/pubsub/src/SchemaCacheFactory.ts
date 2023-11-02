import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { ILogger } from './ILogger'
import { SchemaCache } from './SchemaCache'

export class SchemaCacheFactory {
  private readonly schemaServiceClient: SchemaServiceClient

  constructor(private readonly logger: ILogger) {
    this.schemaServiceClient = new SchemaServiceClient()
  }

  public getSchemaCache(topicSchemaName: string): SchemaCache {
    return new SchemaCache(this.schemaServiceClient, topicSchemaName, this.logger)
  }
}
