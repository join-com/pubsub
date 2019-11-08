import { Datastore } from '@google-cloud/datastore'
import { EntityManager } from './EntityManager'

export class EntityRepository<T = unknown> {
  private entityManager: EntityManager<T>

  constructor(readonly entity: string, readonly client: Datastore) {
    this.entityManager = new EntityManager(entity, client)
  }

  public async set(id: string, data: T): Promise<void> {
    return this.entityManager.set(id, data)
  }

  public async get(id: string): Promise<T | undefined> {
    return this.entityManager.get(id)
  }

  public async runInTransaction<U>(
    fn: (manager: EntityManager<T>) => Promise<U> | U
  ): Promise<U> {
    const transaction = this.client.transaction()
    const entityManager = new EntityManager<T>(this.entity, transaction)
    try {
      await transaction.run()
      const result = await fn(entityManager)
      await transaction.commit()
      return result
    } catch (error) {
      await transaction.rollback()
      throw error
    }
  }
}
