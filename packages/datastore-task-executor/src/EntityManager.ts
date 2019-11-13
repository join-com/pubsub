type Key = any
type GetKeyAttrs = [string, string]

export interface IEntityManagerClient {
  key?: (attrs: GetKeyAttrs) => Key
  get: (key: Key | Key[]) => Promise<[any]>
  save: (data: { key: Key; data: any }) => Promise<any>
}

export interface IEntityManager<T> {
  set: (id: string, data: T) => Promise<void>
  get: (id: string) => Promise<T | undefined>
}

export class EntityManager<T = unknown> implements IEntityManager<T> {
  constructor(readonly entity: string, readonly client: IEntityManagerClient) {}

  public async set(id: string, data: T): Promise<void> {
    const key = this.getKey(id)
    await this.client.save({ key, data })
  }

  public async get(id: string): Promise<T | undefined> {
    const key = this.getKey(id)
    const [record] = await this.client.get(key)
    return record
  }

  private getKey(id: string) {
    if (!this.client.key) {
      throw new Error('No "key" method found')
    }
    return this.client.key([this.entity, id])
  }
}
