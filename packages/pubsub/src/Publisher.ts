import { PubSub } from '@google-cloud/pubsub'
import { MessageOptions } from '@google-cloud/pubsub/build/src/topic'
import { Type } from 'avsc'
import { createCallOptions } from './createCallOptions'
import { ILogger } from './ILogger'
import { TopicHandler } from './TopicHandler'

export class Publisher<T = unknown> extends TopicHandler {
  private avroType: Type | undefined

  constructor(readonly topicName: string, client: PubSub, private readonly logger?: ILogger) {
    super(client, topicName)
  }

  public async initialize() {
    try {
      await this.initializeTopic()
      this.avroType = await this.getTopicType()
    } catch (e) {
      this.logger?.error('PubSub: Failed to initialize publisher', e)
      process.abort()
    }
  }

  public async publishMsg(data: T): Promise<void> {
    // Later we want to have only topic with specified schema and remove if block below
    if (!this.avroType) {
      try {
        await this.publishWithLog({ json: data })
      } catch (e) {
        //TODO: catch if possible invalid schema exception
        //it's a corner case when application started without topic schema, and then schema was added to the topic
        //in this case we are trying to get again topic data and resend with the schema if it's appeared
        this.topic = this.client.topic(this.topic.name)
        this.avroType = await this.getTopicType()
        if (!this.avroType) {
          throw e
        }
        await this.sendAvroMessage(data)
      }
      return
    }

    await this.sendAvroMessage(data)
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
    const buffer = this.avroType!.toBuffer(data)
    await this.publishWithLog({ data: buffer })
  }

  private async publishWithLog(message: MessageOptions) {
    const messageId = await this.topic.publishMessage(message)
    this.logger?.info(`PubSub: Message sent for topic: ${this.topicName}:`, { message, messageId })
  }
}
