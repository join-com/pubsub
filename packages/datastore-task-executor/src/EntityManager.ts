import { DatastoreRequest } from '@google-cloud/datastore'

export class EntityManager<T = unknown> {
  constructor(readonly entity: string, readonly client: DatastoreRequest) {}

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
    return this.client.key([this.entity, id])
  }
}
