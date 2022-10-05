import { ISubscriber } from './SubscriberFactory'

export abstract class MessageHandler<T = unknown> {
  protected constructor(private readonly subscriber: ISubscriber<T>) {}

  protected abstract handle(event: T): Promise<void>

  public start() {
    this.subscriber.start(async msg => {
      await this.handle(msg.dataParsed)
      msg.ack()
    })
  }

  public async initialize() {
    await this.subscriber.initialize()
  }

  public async stop() {
    await this.subscriber.stop()
  }
}
