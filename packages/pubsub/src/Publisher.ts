import { readFileSync } from 'fs'
import { PubSub, Topic } from '@google-cloud/pubsub'
import { google } from '@google-cloud/pubsub/build/protos/protos'
import { Schema, Type } from 'avsc'
import { createCallOptions } from './createCallOptions'
import { FieldsProcessor } from './FieldsProcessor'
import { ILogger } from './ILogger'
import { DateType } from './logical-types/DateType'
import { logWarnWhenUndefinedInNullPreserveFields } from './util'
import Encoding = google.pubsub.v1.Encoding

interface IMessageMetadata {
  Event: string
  GeneratorVersion: string
  GeneratorGitRemoteOriginUrl: string
  SchemaType: string
  AvdlSchemaPathInGitRepo: string
  AvdlSchemaGitRemoteOriginUrl: string
  AvdlSchemaVersion: string
  PreserveNull?: string
  OptionalArrayPaths?: string
}

type SchemaWithMetadata = Schema & IMessageMetadata
export const JOIN_PRESERVE_NULL = 'join_preserve_null'
export const JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS = 'join_undefined_or_null_optional_arrays'
export const JOIN_IDEMPOTENCY_KEY = 'join_idempotency_key'

export class Publisher<T = unknown> {
  private readonly topic: Topic
  private readonly topicSchemaName: string

  private readonly writerAvroType: Type
  private readonly readerAvroType: Type
  private readonly optionArrayPaths?: string[]

  private readonly avroMessageMetadata?: Record<string, string>
  private readonly fieldsProcessor = new FieldsProcessor()

  constructor(
    public readonly topicName: string,
    public readonly client: PubSub,
    avroSchemas: { writer: object; reader: object },
    private readonly logger?: ILogger,
  ) {
    const writerAvroSchema: SchemaWithMetadata = avroSchemas.writer as SchemaWithMetadata
    this.writerAvroType = Type.forSchema(writerAvroSchema, { logicalTypes: { 'timestamp-micros': DateType } })
    if (writerAvroSchema.OptionalArrayPaths) {
      this.optionArrayPaths = writerAvroSchema.OptionalArrayPaths.split(',')
    }
    const readerAvroSchema: SchemaWithMetadata = avroSchemas.reader as SchemaWithMetadata
    this.readerAvroType = Type.forSchema(readerAvroSchema, { logicalTypes: { 'timestamp-micros': DateType } })

    this.avroMessageMetadata = this.prepareAvroMessageMetadata(readerAvroSchema)
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

  public async publishMsg(data: T, attributes?: Record<string, string>): Promise<void> {
    await this.sendAvroMessage(data, attributes)
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
    if (!await this.doesTopicHaveSchemaAssigned()) {
      const projectName = process.env['GCLOUD_PROJECT']
      if (!projectName) {
        throw new Error("Can't find GCLOUD_PROJECT env variable, please define it")
      }
      const schema = `projects/${projectName}/schemas/${this.topicSchemaName}`
      await this.topic.setMetadata({ schemaSettings: { schema, encoding: Encoding.JSON }})
      this.logger?.info(`PubSub: Schema ${schema} is assigned to the topic: ${this.topicName}`)
    }
  }

  private async sendAvroMessage(data: T, attributes?: Record<string, string>): Promise<void> {
    let currentMessageMetadata = { ...this.avroMessageMetadata, ...attributes}
    if (this.optionArrayPaths && this.optionArrayPaths.length > 0) {
      const undefinedOrNullOptionalArrays = this.fieldsProcessor.findAndReplaceUndefinedOrNullOptionalArrays(
        data as Record<string, unknown>,
        this.optionArrayPaths,
      )
      if (undefinedOrNullOptionalArrays.length > 0) {
        currentMessageMetadata = { ...currentMessageMetadata }
        currentMessageMetadata[JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS] = undefinedOrNullOptionalArrays.join(',')
      }
    }
    if (!currentMessageMetadata[JOIN_IDEMPOTENCY_KEY]) {
      currentMessageMetadata[JOIN_IDEMPOTENCY_KEY] = crypto.randomUUID()
    }
    const invalidPaths: string[] = []

    if (!this.writerAvroType.isValid(data, { errorHook: path => invalidPaths.push(path.join('.')) })) {
      this.logger?.error(
        `[${this.topicName}] Invalid payload for the specified writer schema, please check that the schema is correct and payload can be encoded with it`,
        { payload: data, schemaMetadata: currentMessageMetadata, invalidPaths },
      )
      throw new Error(`[${this.topicName}] Can't encode the avro message for the topic`)
    }
    if (currentMessageMetadata && currentMessageMetadata[JOIN_PRESERVE_NULL]) {
      logWarnWhenUndefinedInNullPreserveFields(data, currentMessageMetadata[JOIN_PRESERVE_NULL], this.logger)
    }

    const buffer = Buffer.from(this.readerAvroType.toString(data))
    const messageId = await this.topic.publishMessage({ data: buffer, attributes: currentMessageMetadata })
    this.logger?.info(`PubSub: Avro message sent for topic: ${this.topicName}:`, { data, attributes, messageId })
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
    const packageJson = JSON.parse(readFileSync(libPackageJsonPath, 'utf8')) as { version: string }
    return packageJson.version
  }

  private async doesTopicHaveSchemaAssigned(): Promise<boolean> {
    const [metadata] = await this.topic.getMetadata()
    const schemaName = metadata?.schemaSettings?.schema
    return !!schemaName
  }
}
