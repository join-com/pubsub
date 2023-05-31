import { readFileSync } from 'fs'
import { PubSub, Topic } from '@google-cloud/pubsub'
import { MessageOptions } from '@google-cloud/pubsub/build/src/topic'
import { Schema, Type } from 'avsc'
import { createCallOptions } from './createCallOptions'
import { ILogger } from './ILogger'
import { DateType } from './logical-types/DateType'
import { ISchemaType, TopicHandler } from './TopicHandler'

interface IMessageMetadata {
  Event: string,
  GeneratorVersion: string,
  GeneratorGitRemoteOriginUrl: string,
  SchemaType: string
  AvdlSchemaPathInGitRepo:  string,
  AvdlSchemaGitRemoteOriginUrl: string,
  AvdlSchemaVersion: string
}

type SchemaWithMetadata = Schema & IMessageMetadata

export class Publisher<T = unknown> {
  private readonly topic: Topic
  private readonly topicHandler: TopicHandler
  private topicType?: ISchemaType
  private validationType?: ISchemaType
  private writerAvroType?: Type
  private readonly avroMessageMetadata?: Record<string, string>
  private readonly validationSchemaName: string

  constructor(readonly topicName: string, client: PubSub, private readonly logger?: ILogger, writerAvroSchema?: string) {
    if (writerAvroSchema) {
      const writerAvroJsonSchema = JSON.parse(writerAvroSchema) as SchemaWithMetadata
      this.writerAvroType = Type.forSchema(writerAvroJsonSchema, { logicalTypes: { 'timestamp-micros': DateType } })
      this.avroMessageMetadata = this.prepareAvroMessageMetadata(writerAvroJsonSchema)
    }
    this.topic = client.topic(topicName)
    this.validationSchemaName = `report-only-${this.topicName}-generated-avro`
    this.topicHandler = new TopicHandler(client, this.topic)
  }

  public async initialize() {
    try {
      await this.initializeTopic()

      this.topicType = await this.topicHandler.getSchemaTypeFromTopic()
      this.throwErrorIfOnlyReaderOrWriterSchema(this.writerAvroType, this.topicType?.type)

      if (!this.topicType) {
        try {
          this.validationType = await this.topicHandler.getSchemaType(this.validationSchemaName)
        } catch (e) {
          this.logger?.warn('Couldn\'t get schema for message validation against avro schema', e)
        }
      }
    } catch (e) {
      this.logger?.error('PubSub: Failed to initialize publisher', e)
      process.abort()
    }
  }

  private throwErrorIfOnlyReaderOrWriterSchema(writerSchema?: Type, readerSchema?: Type) {
    if (writerSchema && !readerSchema) {
      throw new Error('Writer schema specified for the topic without reader schema')
    }
    if (!writerSchema && readerSchema) {
      throw new Error('Read schema specified for the topic without writer schema')
    }
  }

  public async publishMsg(data: T): Promise<void> {
    // TODO: Later we want to have only topic with specified schema and remove if block below
    if (!this.topicType) {
      try {
        await this.sendJsonMessage({ json: data })
      } catch (e) {
        //it's a corner case when application started without topic schema, and then schema was added to the topic
        //in this case we are trying to get again topic data and resend with the schema if it's appeared
        this.topicType = await this.topicHandler.getSchemaTypeFromTopic()
        if (!this.topicType) {
          throw e
        }
        await this.sendAvroMessage(data)
      }
      this.logWarnIfMessageViolatesSchema(data)
      return
    }

    await this.sendAvroMessage(data)
  }

  private logWarnIfMessageViolatesSchema(data: T) {
    if (this.validationType) {
      try {
        this.validationType.type.toBuffer(data)
      } catch (e) {
        this.logger?.warn('Message violates avro schema that we plan to enforce',
          {data, schemaName: this.validationSchemaName})
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

  private async sendAvroMessage(data: T) {
    // TODO: remove non-null assertion and eslint-disable when avroType will be mandatory on every topic
    // for now we are checking that it's not null before calling sendAvroMessage
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const buffer = Buffer.from(this.writerAvroType!.toString(data))
    const messageId = await this.topic.publishMessage({ data: buffer, attributes: this.avroMessageMetadata })
    this.logger?.info(`PubSub: Avro message sent for topic: ${this.topicName}:`, { data, messageId })
  }

  private async sendJsonMessage(message: MessageOptions) {
    const messageId = await this.topic.publishMessage(message)
    this.logger?.info(`PubSub: JSON Message sent for topic: ${this.topicName}:`, { message, messageId })
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

    return metadata
  }

  private getLibraryVersion(): string {
    const libPackageJsonPath = `${__dirname}/../package.json`
    const packageJson = JSON.parse(readFileSync(libPackageJsonPath, 'utf8')) as { version: string}
    return packageJson.version
  }
}
