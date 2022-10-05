import { PubSub, Topic } from '@google-cloud/pubsub'
import { createCallOptions } from './createCallOptions'
import { ILogger } from './ILogger'

export class Publisher<T = unknown> {
  private readonly topic: Topic

  constructor(readonly topicName: string, client: PubSub, private readonly logger?: ILogger) {
    this.topic = client.topic(topicName)
  }

  public async initialize() {
    try {
      await this.initializeTopic()
    } catch (e) {
      this.logger?.error('PubSub: Failed to initialize publisher', e)
      process.abort()
    }
  }

  public async publishMsg(data: T): Promise<void> {
    const messageId = await this.topic.publishMessage({ json: data })
    this.logger?.info(`PubSub: Message sent for topic: ${this.topicName}:`, { data, messageId })
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
}
