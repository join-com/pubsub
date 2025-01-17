import { JOIN_IDEMPOTENCY_KEY } from './Publisher'
import { IRedisClient } from './redis-types/IRedisClient'
import { IMessageInfo, IParsedMessage } from './Subscriber'
import { ISubscriber } from './SubscriberFactory'

type GetIdempotencyKeyFunction<T> = (msg: T, info: IMessageInfo) => string | undefined

export interface IIdempotencyStorage {
  exists(key: string): Promise<boolean>

  save(key: string): Promise<void>
}

export class RedisIdempotencyStorage implements IIdempotencyStorage {
  constructor(private readonly redisClient: IRedisClient,
              private readonly redisDefaultTtl: number) {}

  public async exists(key: string): Promise<boolean> {
    const value = await this.redisClient.get(key)
    if (value.success) {
      return !!value.get()
    } else {
      return false
    }
  }

  public async save(key: string): Promise<void> {
    const value = Buffer.from(new Date().toISOString())
    await this.redisClient.setex(key, this.redisDefaultTtl, value)
  }
}

/**
 * Idempotent message handler, requires idempotency storage to be provided.
 * Will check if message was already processed by checking idempotency key in the storage, and will skip it if it was.
 * After the message is processed, stores message in the idempotency storage
 * If no idempotency key is provided, message is processed without idempotency check
 */
export abstract class IdempotentMessageHandler<T = unknown> {
  /**
   *
   * @param subscriber subscriber to listen to
   * @param idempotencyStorage storage to check if message was already processed, planned to be used with redis
   * @param getIdempotencyKey value from message or attributes, by default uses join_idempotency_key, but can be overridden
   * @protected
   */
  private noIdempotencyKeyErrorShown = false
  
  protected constructor(
    private readonly subscriber: ISubscriber<T>,
    private readonly idempotencyStorage: IIdempotencyStorage,
    private readonly getIdempotencyKey: GetIdempotencyKeyFunction<T> =
      (_: T, info: IMessageInfo) => {
        return info?.attributes[JOIN_IDEMPOTENCY_KEY]
      }) {
  }

  protected abstract handle(event: T, info: IMessageInfo): Promise<void>

  public start(): void {
    this.subscriber.start(async (msg: IParsedMessage<T>, info: IMessageInfo) => {
      const idempotencyKey = this.getIdempotencyKeyWithLog(msg.dataParsed, info)
      if (idempotencyKey) {
        const alreadyProcessed = await this.idempotencyStorage.exists(idempotencyKey)
          .catch(e => {
            this.subscriber.logger?.info(`Error checking idempotency key existence: ${idempotencyKey}`, e)
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

  /**
   * Logging is added to have some notification in logs if idempotency key is not provided, and 
   * idempotent handler just works as usual handler, but to not spam with every message we log it once
   */
  private getIdempotencyKeyWithLog(msg: T, info: IMessageInfo): string | undefined {
    const idempotencyKey = this.getIdempotencyKey(msg, info)
    if (!idempotencyKey && !this.noIdempotencyKeyErrorShown) {
      this.subscriber.logger?.error('First message received without idempotency key in idempotent handler, ' +
        'possibly publisher pubsub version bump or idempotency key set is needed, or old messages are processed')
      this.noIdempotencyKeyErrorShown = true
    }
    return idempotencyKey
  }
}
