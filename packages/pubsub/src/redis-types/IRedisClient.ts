/**
 * This file contains interfaces from https://github.com/join-com/caching ,
 * for a less duplicated code when implementing idempotent handler on services.
 */

export interface IFailure<T> {
  success: false
  failure: true

  get(): T
}

export interface ISuccess<T> {
  success: true
  failure: false

  get(): T
}

export type RedisResult<T, R> = ISuccess<T> | IFailure<R>

export type OptionalRedisResult<T, R> = ISuccess<T> | ISuccess<undefined> | IFailure<R>

export interface IRedisError {

  error: Error
}


export interface IRedisClient {

  get(key: string): Promise<OptionalRedisResult<Buffer, IRedisError>>

  setex(key: string, secondsTtl: number, value: Buffer): Promise<RedisResult<void, IRedisError>>
}
