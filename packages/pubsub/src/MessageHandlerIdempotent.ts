import { JOIN_IDEMPOTENCY_KEY } from './Publisher'
import { IMessageInfo, IParsedMessage } from './Subscriber'
import { ISubscriber } from './SubscriberFactory'

type GetIdempotencyKeyFunction<T> = (msg: T, info: IMessageInfo) => string | undefined

export abstract class IdempotencyStorage {
  public abstract exists(key: string): Promise<boolean>

  public abstract save(key: string): Promise<void>
}

/**
 * Idempotent message handler, requires idempotency storage to be provided.
 * Will check if message was already processed by checking idempotency key in the storage, and will skip it if it was.
 * After the message is processed, stores message in the idempotency storage
 */
export abstract class MessageHandlerIdempotent<T = unknown> {
  /**
   *
   * @param subscriber subscriber to listen to
   * @param idempotencyStorage storage to check if message was already processed, planned to be used with redis
   * @param getIdempotencyKey value from message or attributes, by default uses join_idempotency_key, but can be overridden
   * @protected
   */
  protected constructor(
    private readonly subscriber: ISubscriber<T>,
    private readonly idempotencyStorage: IdempotencyStorage,
    private readonly getIdempotencyKey: GetIdempotencyKeyFunction<T> =
      (_: T, info: IMessageInfo) => {
        return info.attributes[JOIN_IDEMPOTENCY_KEY]
      }) {
  }

  protected abstract handle(event: T, info: IMessageInfo): Promise<void>

  public start(): void {
    this.subscriber.start(async (msg: IParsedMessage<T>, info: IMessageInfo) => {
      const idempotencyKey = this.getIdempotencyKey(msg.dataParsed, info)
      if (idempotencyKey) {
        const alreadyProcessed = await this.idempotencyStorage.exists(idempotencyKey)
          .catch(e => {
            this.subscriber.logger?.info(`Error checking idempotency key: ${idempotencyKey}`, e)
            return false
          })
        if (alreadyProcessed) {
          msg.ack()
          return
        }
      }

      await this.handle(msg.dataParsed, info)
      if (idempotencyKey) {
        this.idempotencyStorage.save(idempotencyKey)
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
