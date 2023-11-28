import { readFileSync } from 'fs'
import { PubSub, Topic } from '@google-cloud/pubsub'
import { google } from '@google-cloud/pubsub/build/protos/protos'
import { MessageOptions } from '@google-cloud/pubsub/build/src/topic'
import { Schema, Type } from 'avsc'
import { createCallOptions } from './createCallOptions'
import { FieldsProcessor } from './FieldsProcessor'
import { ILogger } from './ILogger'
import { DateType } from './logical-types/DateType'
import { logWarnWhenUndefinedInNullPreserveFields } from './util'
import Encoding = google.pubsub.v1.Encoding

interface IMessageMetadata {
  Event: string,
  GeneratorVersion: string,
  GeneratorGitRemoteOriginUrl: string,
  SchemaType: string
  AvdlSchemaPathInGitRepo:  string,
  AvdlSchemaGitRemoteOriginUrl: string,
  AvdlSchemaVersion: string,
  PreserveNull?: string
  OptionalArrayPaths?: string
}

type SchemaWithMetadata = Schema & IMessageMetadata
export const JOIN_PRESERVE_NULL = 'join_preserve_null'
export const JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS = 'join_undefined_or_null_optional_arrays'

export class Publisher<T = unknown> {
  private readonly topic: Topic
  private readonly topicSchemaName: string

  private readonly writerAvroType?: Type
  private readonly readerAvroType?: Type
  private readonly optionArrayPaths?: string[]

  private readonly avroMessageMetadata?: Record<string, string>
  private readonly fieldsProcessor = new FieldsProcessor()
  //TODO: remove flags below, when only avro will be used
  private topicHasAssignedSchema = false
  private avroSchemasProvided = false

  constructor(readonly topicName: string, readonly client: PubSub, private readonly logger?: ILogger,
              avroSchemas?: { writer: object, reader: object }) {
    //TODO: avroSchemas parameter should be mandatory when only avro is used
    if (avroSchemas) {
      this.avroSchemasProvided = true
      const writerAvroSchema: SchemaWithMetadata = avroSchemas.writer as SchemaWithMetadata
      this.writerAvroType = Type.forSchema(writerAvroSchema, { logicalTypes: { 'timestamp-micros': DateType } })
      if (writerAvroSchema.OptionalArrayPaths) {
        this.optionArrayPaths = writerAvroSchema.OptionalArrayPaths.split(',')
      }
      const readerAvroSchema: SchemaWithMetadata = avroSchemas.reader as SchemaWithMetadata
      this.readerAvroType = Type.forSchema(readerAvroSchema, { logicalTypes: { 'timestamp-micros': DateType } })

      this.avroMessageMetadata = this.prepareAvroMessageMetadata(readerAvroSchema)
    }
    this.topic = client.topic(topicName)
    this.topicSchemaName = `${this.topicName}-generated-avro`
  }

  public async initialize(): Promise<void> {
    try {
      await this.initializeTopic()
      await this.initializeTopicSchema()
    } catch (e) {
      this.logger?.error('PubSub: Failed to initialize publisher', e)
      process.abort()
    }
  }

  public async publishMsg(data: T): Promise<void> {
    if (!this.avroSchemasProvided) {
      // old flow, just send message if no avro schemas provided
      await this.sendJsonMessage({ json: data })
    } else if (!this.topicHasAssignedSchema) {
      try {
        await this.sendJsonMessage({ json: data })
        this.logWarnIfMessageViolatesSchema(data)
      } catch (e) {
        //it's a corner case when application started without schema on topic, and then schema was added to the topic
        //in this case we are trying to resend message with avro format if schema appeared
        this.topicHasAssignedSchema = await this.doesTopicHaveSchemaAssigned()
        if (!this.topicHasAssignedSchema) {
          throw e
        }
        await this.sendAvroMessage(data)
      }
    } else {
      // TODO: remove everything except this call after services will be ready to use only avro
      await this.sendAvroMessage(data)
    }
  }

  private logWarnIfMessageViolatesSchema(data: T): void {
    if (this.writerAvroType) {
      const invalidPaths: string[] = []
      if (!this.writerAvroType.isValid(data, {errorHook: path => invalidPaths.push(path.join('.'))} )) {
        this.logger?.warn(`[schema-violation] [${this.topicName}] Message violates writer avro schema`, { payload: data, metadata: this.avroMessageMetadata, invalidPaths })
      }
    }
  }

  public async flush(): Promise<void> {
    this.logger?.info(`PubSub: Flushing messages for topic: ${this.topicName}:`)
    await this.topic.flush()
  }

  private async initializeTopic() {
    const [exist] = await this.topic.exists()
    this.logger?.info(`PubSub: Topic ${this.topicName} ${exist ? 'exists' : 'does not exist'}`)

    if (!exist) {
      await this.topic.create(createCallOptions)
      this.logger?.info(`PubSub: Topic ${this.topicName} is created`)
    }
  }

  private async initializeTopicSchema(): Promise<void> {
    if (this.avroSchemasProvided) {
      this.topicHasAssignedSchema = await this.doesTopicHaveSchemaAssigned()

      if (!this.topicHasAssignedSchema && await this.doesRegistryHaveTopicSchema()) {
        // TODO: this.setSchemaToTheTopic() should be replace with
        // ```await this.topic.setMetadata({ schemaSettings: { schema: this.topicSchemaName, encoding: Encoding.JSON }})
        // this.topicHasAssignedSchema = true```
        // once https://github.com/googleapis/nodejs-pubsub/issues/1587 is fixed
        this.setSchemaToTheTopic()
      }
    }
  }

  private setSchemaToTheTopic() {
    const projectName = process.env['GCLOUD_PROJECT']
    if (!projectName) {
      throw new Error('Can\'t find GCLOUD_PROJECT env variable, please define it')
    }

    this.topic.request(
      {
        client: 'PublisherClient',
        method: 'updateTopic',
        reqOpts: {
          topic: {
            name: `projects/${projectName}/topics/${this.topicName}`,
            schemaSettings: {
              schema: `projects/${projectName}/schemas/${this.topicSchemaName}`,
              encoding: Encoding.JSON,
            },
          },
          updateMask: {
            paths: ['schema_settings'],
          },
        },
        gaxOpts: {},
      },
      (err, _) => {
        if (!err) {
          this.topicHasAssignedSchema = true
          this.logger?.info(`Schema '${this.topicSchemaName}' set to the topic '${this.topicName}'`)
        } else {
          this.logger?.error(`Couldn't set schema '${this.topicSchemaName}'  to the topic '${this.topicName}'`)
        }
      },
    )
  }

  private async sendAvroMessage(data: T): Promise<void> {
    let currentMessageMetadata = this.avroMessageMetadata
    if (this.optionArrayPaths && this.optionArrayPaths.length > 0) {
      const undefinedOrNullOptionalArrays = this.fieldsProcessor.findAndReplaceUndefinedOrNullOptionalArrays(data as Record<string, unknown>, this.optionArrayPaths)
      if (undefinedOrNullOptionalArrays.length > 0) {
        currentMessageMetadata = { ...currentMessageMetadata }
        currentMessageMetadata[JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS] = undefinedOrNullOptionalArrays.join(',')
      }
    }
    // TODO: remove non-null assertion and eslint-disable when avroType will be mandatory on every topic
    // for now we are checking that avro is enabled before calling sendAvroMessage
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    if (!this.writerAvroType!.isValid(data)) {
      this.logger?.error(`[${this.topicName}] Invalid payload for the specified writer schema, please check that the schema is correct ' +
        'and payload can be encoded with it`, {payload: data, schemaMetadata: currentMessageMetadata})
      throw new Error(`[${this.topicName}] Can't encode the avro message for the topic`)
    }
    if (currentMessageMetadata && currentMessageMetadata[JOIN_PRESERVE_NULL]) {
      logWarnWhenUndefinedInNullPreserveFields(data, currentMessageMetadata[JOIN_PRESERVE_NULL], this.logger)
    }

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const buffer = Buffer.from(this.readerAvroType!.toString(data))
    const messageId = await this.topic.publishMessage({ data: buffer, attributes: currentMessageMetadata })
    this.logger?.info(`PubSub: Avro message sent for topic: ${this.topicName}:`, { data, messageId })
  }



  private async sendJsonMessage(message: MessageOptions) {
    const messageId = await this.topic.publishMessage(message)
    this.logger?.info(`PubSub: JSON Message sent for topic: ${this.topicName}:`, { data: message.json as unknown, messageId })
  }

  private prepareAvroMessageMetadata(schema: SchemaWithMetadata): Record<string, string> {
    const metadata: Record<string, string> = {}

    metadata['join_event'] = schema.Event
    metadata['join_generator_version'] = schema.GeneratorVersion
    metadata['join_generator_git_remote_origin_url'] = schema.GeneratorGitRemoteOriginUrl
    metadata['join_schema_type'] = schema.SchemaType
    metadata['join_avdl_schema_path_in_git_repo'] = schema.AvdlSchemaPathInGitRepo
    metadata['join_avdl_schema_git_remote_origin_url'] = schema.AvdlSchemaGitRemoteOriginUrl
    metadata['join_avdl_schema_version'] = schema.AvdlSchemaVersion
    metadata['join_pubsub_lib_version'] = this.getLibraryVersion()
    if (schema.PreserveNull) {
      metadata[JOIN_PRESERVE_NULL] = schema.PreserveNull
    }

    return metadata
  }

  private getLibraryVersion(): string {
    const libPackageJsonPath = `${__dirname}/../package.json`
    const packageJson = JSON.parse(readFileSync(libPackageJsonPath, 'utf8')) as { version: string}
    return packageJson.version
  }

  private async doesTopicHaveSchemaAssigned(): Promise<boolean> {
    const [metadata] = await this.topic.getMetadata()
    const schemaName = metadata?.schemaSettings?.schema
    return !!schemaName
  }

  public async doesRegistryHaveTopicSchema(): Promise<boolean> {
    try {
      return !!(await this.client.schema(this.topicSchemaName).get())
    } catch (e) {
      this.logger?.info(`Schema ${this.topicSchemaName} can't be found`, e)
      return false
    }
  }
}
