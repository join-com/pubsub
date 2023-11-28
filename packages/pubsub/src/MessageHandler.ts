import { IMessageInfo, IParsedMessage } from './Subscriber'
import { ISubscriber } from './SubscriberFactory'

export abstract class MessageHandler<T = unknown> {
  protected constructor(private readonly subscriber: ISubscriber<T>) {}

  protected abstract handle(event: T , info: IMessageInfo): Promise<void>

  public start(): void {
    this.subscriber.start(async (msg:IParsedMessage<T>, info:IMessageInfo) => {
      await this.handle(msg.dataParsed, info)
      msg.ack()
    })
  }

  public async initialize(): Promise<void> {
    await this.subscriber.initialize()
  }

  public async stop(): Promise<void> {
    await this.subscriber.stop()
  }
}
