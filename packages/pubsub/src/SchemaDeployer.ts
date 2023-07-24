import { PubSub } from '@google-cloud/pubsub';
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { ILogger } from './ILogger'

interface ISchemasForDeployment {
  forCreate: Map<string, string>,
  forNewRevision: Map<string, string>
  forDelete: string[]
}
interface IDeploymentResult {
  schemasCreated: number,
  revisionsCreated: number,
  schemasDeleted: number
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
                                    topicsReaderSchemas: Record<string, ReaderAvroSchema>): Promise<IDeploymentResult> => {
    if (!process.env['GCLOUD_PROJECT']) {
      throw new Error('Can\'t find GCLOUD_PROJECT env variable, please define it')
    }

    const { forCreate, forNewRevision, forDelete } = await this.aggregateTopicSchemas(topicsReaderSchemas, topicsSchemaConfig);
    if (forCreate.size === 0 && forNewRevision.size === 0 && forDelete.length === 0) {
      this.logger.info('Finished deployAvroSchemas, all schemas are already deployed')
      return {schemasCreated: 0, revisionsCreated: 0, schemasDeleted: 0}

    }
    this.logger.info(`Found ${forCreate.size} not deployed schemas, and ${forNewRevision.size} new revisions, starting deployment`)

    await this.createSchemas(forCreate)
    await this.createRevisions(forNewRevision)
    await this.deleteSchemas(forDelete)

    this.logger.info(`Schemas deployment is finished, ${forCreate.size} schemas and ${forNewRevision.size} revisions are created`)

    return {schemasCreated: forCreate.size, revisionsCreated: forNewRevision.size, schemasDeleted: forDelete.length}

  }

  private async createRevisions(forNewRevision: Map<string, string>) : Promise<void> {
    const projectName = process.env['GCLOUD_PROJECT'] as string
    for (const [topicSchema, definition] of forNewRevision) {
      const schemaName = topicSchema
      const schemaPath = `projects/${projectName}/schemas/${schemaName}`
      await this.schemaClient.commitSchema({
        name: schemaPath, schema: {
          name: schemaPath, type: AVRO, definition,
        },
      })
      this.logger.info(`Schema ${schemaName} is updated`)
    }
  }

  private async createSchemas(forCreate: Map<string, string>): Promise<void> {
    for (const [schemaName, definition] of forCreate) {
      await this.pubSubClient.createSchema(schemaName, AVRO, definition)
      this.logger.info(`Schema ${schemaName} is created`)
    }
  }

  private async deleteSchemas(schemas: string[]): Promise<void> {
    for (const schema of schemas) {
      await this.schemaClient.deleteSchema({name: schema})
      this.logger.info(`Schema ${schema} is deleted`)
    }
  }

  private async aggregateTopicSchemas(topicSchemasToDeploy: Record<string, ReaderAvroSchema>, topicsSchemaConfig: Record<string, boolean>)
    : Promise<ISchemasForDeployment> {
    const forCreate = new Map<string, string>()
    const forDeleteSet = new Set<string>()
    for (const [topic, schema] of Object.entries(topicSchemasToDeploy)) {
      if (topicsSchemaConfig[topic] === true) {
        forCreate.set(topic + SCHEMA_NAME_SUFFIX, JSON.stringify(schema.reader))
      } else if (topicsSchemaConfig[topic] === false) {
        forDeleteSet.add(topic + SCHEMA_NAME_SUFFIX)
      } else {
        throw new Error(`Unknown value in topicsSchemaConfig ${JSON.stringify(topicsSchemaConfig)}`)
      }
    }

    const forDelete: string[] = []
    const forNewRevision = new Map<string, string>()
    for await (const schema of this.pubSubClient.listSchemas('FULL')) {
      if (!schema.name) {
        this.logger.warn('Found schema without name')
        continue
      }
      if (!schema.name.endsWith(SCHEMA_NAME_SUFFIX)) {
        continue
      }
      if (forCreate.has(schema.name)) {
        if (schema.type != AVRO) {
          throw new Error(`Non avro schema exists for avro topic '${schema.name}', please remove it before starting the service`)
        }
        const newDefinition = forCreate.get(schema.name);
        if (newDefinition && schema.definition !== newDefinition) {
          forNewRevision.set(schema.name, newDefinition)
        }
        forCreate.delete(schema.name)
      }
      if (forDeleteSet.has(schema.name)) {
        forDelete.push(schema.name)
      }
    }
    return { forCreate, forNewRevision, forDelete }
  }
}
