import { logger, reportError } from '@join-com/gcloud-logger-trace'
import { getTraceContext, getTraceContextName } from '@join-com/node-trace'
import { PubSub, Topic } from '@google-cloud/pubsub'

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
    const message = !Array.isArray(data)
      ? Object.assign({}, data, attributes) // For backward compatibility. Attributes assignment should be removed after all pubsub subscribers migrated
      : data
    const messageId = await this.topic.publishJSON(message, attributes)

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

  private getAttributes() {
    const traceContext = getTraceContext()
    if (!traceContext) {
      logger.warn('No trace context defined')
      return undefined
    }

    const traceContextName = getTraceContextName()
    return { [traceContextName]: traceContext }
  }
}
