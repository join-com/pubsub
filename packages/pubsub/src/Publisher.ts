import { PubSub, Topic } from '@google-cloud/pubsub'
import { MessageOptions } from '@google-cloud/pubsub/build/src/topic'
import { createCallOptions } from './createCallOptions'
import { ILogger } from './ILogger'
import { ISchemaType, TopicHandler } from './TopicHandler'

export class Publisher<T = unknown> {
  private readonly topic: Topic
  private readonly topicHandler: TopicHandler
  private topicType: ISchemaType | undefined
  private validationType: ISchemaType | undefined
  private readonly validationSchemaName: string

  constructor(readonly topicName: string, client: PubSub, private readonly logger?: ILogger) {
    this.topic = client.topic(topicName)
    this.validationSchemaName = `report-only-${this.topicName}-generated-avro`
    this.topicHandler = new TopicHandler(client, this.topic)
  }

  public async initialize() {
    try {
      await this.initializeTopic()
      this.topicType = await this.topicHandler.getTopicType()
      if (!this.topicType) {
        this.validationType = await this.topicHandler.getSchemaType(this.validationSchemaName)
      }
    } catch (e) {
      this.logger?.error('PubSub: Failed to initialize publisher', e)
      process.abort()
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
        this.topicType = await this.topicHandler.getTopicType()
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
    //TODO: remove non-null assertion and eslint-disable when avroType will be mandatory on every topic
    // for now we are checking that it's not null before calling sendAvroMessage
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const buffer = this.topicType!.type.toBuffer(data)
    const messageId = await this.topic.publishMessage({ data: buffer })
    this.logger?.info(`PubSub: Avro message sent for topic: ${this.topicName}:`, { data, messageId })

  }

  private async sendJsonMessage(message: MessageOptions) {
    const messageId = await this.topic.publishMessage(message)
    this.logger?.info(`PubSub: JSON Message sent for topic: ${this.topicName}:`, { message, messageId })
  }
}
