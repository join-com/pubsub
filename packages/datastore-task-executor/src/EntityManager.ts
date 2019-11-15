import { IEntityKeyManager, IKey } from './EntityKeyManager'

export interface IRequestClient {
  get: (key: IKey | IKey[]) => Promise<[any]>
  save: (data: { key: IKey; data: any }) => Promise<any>
}

export interface IEntityManager<T> {
  set: (id: string, data: T) => Promise<void>
  get: (id: string) => Promise<T | undefined>
}

export class EntityManager<T = unknown> implements IEntityManager<T> {
  constructor(
    readonly keyManger: IEntityKeyManager,
    readonly requestClient: IRequestClient
  ) {}

  public async set(id: string, data: T): Promise<void> {
    const key = this.keyManger.getKey(id)
    await this.requestClient.save({ key, data })
  }

  public async get(id: string): Promise<T | undefined> {
    const key = this.keyManger.getKey(id)
    const [record] = await this.requestClient.get(key)
    return record
  }
}
