import { JOIN_IDEMPOTENCY_KEY } from './Publisher'
import { IMessageInfo, IParsedMessage } from './Subscriber'
import { ISubscriber } from './SubscriberFactory'

export abstract class IdempotencyStorage {
  public abstract exists(key: string): Promise<boolean>

  public abstract save(key: string): Promise<void>
}

export abstract class MessageHandlerIdempotent<T = unknown> {
  protected constructor(
    private readonly subscriber: ISubscriber<T>,
    private readonly idempotencyStorage: IdempotencyStorage,
    private readonly getIdempotencyKey: (parsedMessage: T, info: IMessageInfo) => string | undefined =
      (_: T, info: IMessageInfo) => {
        return info.attributes[JOIN_IDEMPOTENCY_KEY]
      }) {
  }

  protected abstract handle(event: T, info: IMessageInfo): Promise<void>

  public start(): void {
    this.subscriber.start(async (msg: IParsedMessage<T>, info: IMessageInfo) => {
      const idempotencyKey = this.getIdempotencyKey(msg.dataParsed, info)
      if (idempotencyKey) {
        let processed = false
        try {
          processed = await this.idempotencyStorage.exists(idempotencyKey)
        } catch (e) {
          this.subscriber.logger?.info(`Error checking idempotency key: ${idempotencyKey}`, e)
        }
        if (processed) {
          msg.ack()
          return
        }
      }

      await this.handle(msg.dataParsed, info)
      if (idempotencyKey) {
        this.idempotencyStorage.save(idempotencyKey)
          .then(_ => {
            return
          })
          .catch(e => {
            throw e
          })
      }
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
