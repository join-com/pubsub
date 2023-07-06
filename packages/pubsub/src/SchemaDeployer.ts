import { PubSub } from '@google-cloud/pubsub';
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { ILogger } from './ILogger'

interface ISchemasForDeployment {
  forCreate: Map<string, string>,
  forNewRevision: Map<string, string>
}
interface ISchemaWithEvent {
  Event: string
}
interface IDeploymentResult {
  schemasCreated: number,
  revisionsCreated: number
}
const AVRO = 'AVRO'

export const SCHEMA_NAME_SUFFIX = '-generated-avro'
export type ReaderAvroSchema = {
  reader: object
}
export class SchemaDeployer {

  constructor(private readonly logger: ILogger,
              private readonly pubSubClient: PubSub = new PubSub(),
              private readonly schemaClient: SchemaServiceClient = new SchemaServiceClient()) {
  }
  public deployAvroSchemas = async (topicsSchemaConfig: Record<string, boolean>,
                                    topicReaderSchemas: Record<string, ReaderAvroSchema>): Promise<IDeploymentResult> => {
    if (!process.env['GCLOUD_PROJECT']) {
      throw new Error('Can\'t find GCLOUD_PROJECT env variable, please define it')
    }
    const topicSchemasToDeploy = this.getEnabledTopicSchemas(topicsSchemaConfig, topicReaderSchemas);
    if (topicSchemasToDeploy.size === 0) {
      this.logger.info('Finished deployAvroSchemas, no schemas to deploy')
      return {schemasCreated: 0, revisionsCreated: 0}
    }
    this.logger.info(`Found ${topicSchemasToDeploy.size} schemas enabled for deployment`)

    const { forCreate, forNewRevision } = await this.aggregateTopicSchemas(topicSchemasToDeploy, topicsSchemaConfig);
    if (forCreate.size === 0 && forNewRevision.size === 0) {
      this.logger.info('Finished deployAvroSchemas, all schemas are already deployed')
      return {schemasCreated: 0, revisionsCreated: 0}

    }
    this.logger.info(`Found ${forCreate.size} not deployed schemas, and ${forNewRevision.size} new revisions, starting deployment`)

    await this.createSchemas(forCreate)
    await this.createRevisions(forNewRevision)

    this.logger.info(`Schemas deployment is finished, ${forCreate.size} schemas and ${forNewRevision.size} revisions are created`)

    return {schemasCreated: forCreate.size, revisionsCreated: forNewRevision.size}

  }

  private async createRevisions(forNewRevision: Map<string, string>) {
    const projectName = process.env['GCLOUD_PROJECT'] as string
    for (const [topicSchema, definition] of forNewRevision) {
      const schemaName = topicSchema + SCHEMA_NAME_SUFFIX
      const schemaPath = `projects/${projectName}/schemas/${schemaName}`
      await this.schemaClient.commitSchema({
        name: schemaPath, schema: {
          name: schemaPath, type: AVRO, definition,
        },
      })
      this.logger.info(`Schema ${schemaName} is updated`)
    }
  }

  private async createSchemas(forCreate: Map<string, string>) {
    for (const [topicSchema, definition] of forCreate) {
      const schemaName = topicSchema + SCHEMA_NAME_SUFFIX
      await this.pubSubClient.createSchema(schemaName, AVRO, definition)
      this.logger.info(`Schema ${schemaName} is created`)
    }
  }

  private getEnabledTopicSchemas(schemasConfig: Record<string, boolean>, readerSchemas: Record<string, ReaderAvroSchema>)
    : Map<string, string> {
    const enabledTopicsSchemas = new Map<string, string>()
    for (const topicName in schemasConfig) {
      const readerSchema = readerSchemas[topicName]
      if (schemasConfig[topicName] && readerSchema) {
        enabledTopicsSchemas.set(topicName, JSON.stringify(readerSchema.reader))
      }
    }
    return enabledTopicsSchemas;
  }

  private async aggregateTopicSchemas(topicSchemasToDeploy: Map<string, string>, topicsSchemaConfig: Record<string, boolean>)
    : Promise<ISchemasForDeployment> {
    const forCreate = new Map<string, string>(topicSchemasToDeploy)
    const forNewRevision = new Map<string, string>()
    for await (const schema of this.pubSubClient.listSchemas('FULL')) {
      if (schema.type != AVRO) {
        if (schema.name && topicsSchemaConfig[schema.name]) {
          throw new Error(`Non avro schema exists for avro topic '${schema.name}', please remove it before starting the service`)
        }
      }
      const definition = schema.definition
      if (!definition) {
        if (schema.name) {
          this.logger.warn(`Schema without definition: ${schema.name}`)
        } else {
          this.logger.warn('Found schema without name and definition')
        }
        continue
      }
      const parsedDefinition = JSON.parse(definition) as ISchemaWithEvent
      const eventName = parsedDefinition.Event
      if (topicSchemasToDeploy.has(eventName)) {
        forCreate.delete(eventName)
        const newDefinition = topicSchemasToDeploy.get(eventName);
        if (newDefinition && definition !== newDefinition) {
          forNewRevision.set(eventName, newDefinition)
        }
      }
    }
    return { forCreate, forNewRevision }
  }
}
