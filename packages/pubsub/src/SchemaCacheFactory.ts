import { SchemaServiceClient, SubscriberClient } from '@google-cloud/pubsub/build/src/v1'
import { ILogger } from './ILogger'
import { SchemaCache } from './SchemaCache'

export class SchemaCacheFactory {
  private readonly schemaServiceClient: SchemaServiceClient
  private readonly subscriptionClient: SubscriberClient

  constructor(private readonly logger: ILogger) {
    this.schemaServiceClient = new SchemaServiceClient()
    this.subscriptionClient = new SubscriberClient()
  }

  public getSchemaCache(topicSchemaName: string): SchemaCache {
    return new SchemaCache(this.schemaServiceClient, this.subscriptionClient, topicSchemaName, this.logger)
  }
}
