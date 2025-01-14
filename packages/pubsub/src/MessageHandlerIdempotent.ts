import { JOIN_IDEMPOTENCY_KEY } from './Publisher'
import { IMessageInfo, IParsedMessage } from './Subscriber'
import { ISubscriber } from './SubscriberFactory'

export abstract class MessageHandlerIdempotent<T = unknown> {
  protected constructor(private readonly subscriber: ISubscriber<T>) {
  }

  protected abstract handle(event: T, info: IMessageInfo): Promise<void>

  protected abstract existsInStore(idempotencyKey: string): Promise<boolean>

  protected abstract saveInStore(key: string): Promise<void>

  public start(): void {
    this.subscriber.start(async (msg: IParsedMessage<T>, info: IMessageInfo) => {
      const idempotencyKey = this.getIdempotencyKey(msg.dataParsed, info)
      if (idempotencyKey) {
        const processed = await this.existsInStore(idempotencyKey)
        if (processed) {
          msg.ack()
          return
        }
      }

      await this.handle(msg.dataParsed, info)
      msg.ack()

      if (idempotencyKey) {
        this.saveInStore(idempotencyKey)
          .then(_ => {
            return
          })
          .catch(e => {
            throw e
          })
      }
    })
  }

  public async initialize(): Promise<void> {
    await this.subscriber.initialize()
  }

  public async stop(): Promise<void> {
    await this.subscriber.stop()
  }

  protected getIdempotencyKey(_: T, info: IMessageInfo): string | undefined {
    return info.attributes[JOIN_IDEMPOTENCY_KEY]
  }
}
