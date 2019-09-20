import { logger, reportError } from '@join-com/gcloud-logger-trace'
import { getTraceContext, getTraceContextName } from '@join-com/node-trace'
import { PubSub, Topic } from '@google-cloud/pubsub'

export class Publisher<T = unknown> {
  private topic: Topic

  constructor(topicName: string, client: PubSub) {
    this.topic = client.topic(topicName)
  }

  public async initialize() {
    try {
      await this.initializeTopic(this.topic)
    } catch (e) {
      reportError(e)
      process.exit(1)
    }
  }

  public async publishMsg(data: T): Promise<void> {
    const traceContext = getTraceContext()
    const traceContextName = getTraceContextName()
    const messageId = await this.topic.publishJSON({
      ...data,
      [traceContextName]: traceContext
    })

    logger.info(`PubSub: Message sent for topic: ${this.topic.name}:`, {
      data,
      messageId
    })
  }

  private async initializeTopic(topic: Topic) {
    const [exist] = await topic.exists()
    logger.info(
      `PubSub: Topic ${topic.name} ${exist ? 'exists' : 'does not exist'}`
    )

    if (!exist) {
      await topic.create()
      logger.info(`PubSub: Topic ${topic.name} is created`)
    }
  }
}
