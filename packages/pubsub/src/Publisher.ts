import { Attributes, PubSub, Topic } from '@google-cloud/pubsub'
import { logger } from '@join-com/gcloud-logger-trace'
import { reportError } from './reportError'
import { getTraceContext, getTraceContextName } from '@join-com/node-trace'

export class Publisher<T = unknown> {
  private topic: Topic

  constructor(readonly topicName: string, client: PubSub) {
    this.topic = client.topic(topicName)
  }

  public async initialize() {
    try {
      await this.initializeTopic()
    } catch (e) {
      reportError(e)
      process.exit(1)
    }
  }

  public async publishMsg(data: T): Promise<void> {
    const attributes = this.getAttributes()
    const messageId = await this.topic.publishJSON(data as any, attributes)

    logger.info(`PubSub: Message sent for topic: ${this.topicName}:`, {
      data,
      messageId
    })
  }

  private async initializeTopic() {
    const [exist] = await this.topic.exists()
    logger.info(
      `PubSub: Topic ${this.topicName} ${exist ? 'exists' : 'does not exist'}`
    )

    if (!exist) {
      await this.topic.create()
      logger.info(`PubSub: Topic ${this.topicName} is created`)
    }
  }

  private getAttributes(): Attributes {
    const traceContext = getTraceContext()
    if (!traceContext) {
      logger.warn('No trace context defined')
      return {}
    }

    const traceContextName = getTraceContextName()
    return { [traceContextName]: traceContext }
  }
}
