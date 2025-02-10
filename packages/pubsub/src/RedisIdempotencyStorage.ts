import { IRedisClient } from './redis-types/IRedisClient'

export interface IIdempotencyStorage {
  exists(key: string): Promise<boolean>

  save(key: string): Promise<void>
}

export class RedisIdempotencyStorage implements IIdempotencyStorage {
  /**
   * Constructor
   * @param redisClient implementation of IRedisClient
   * @param redisDefaultSecondsTtl redis default Time-to-Live for the message key, by default set to 2 hours
   */
  constructor(private readonly redisClient: IRedisClient,
              private readonly redisDefaultSecondsTtl: number = 7200) {}

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
    await this.redisClient.setex(key, this.redisDefaultSecondsTtl, value)
  }
}
