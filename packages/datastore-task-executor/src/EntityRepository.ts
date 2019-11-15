import { EntityKeyManager, IKeyClient } from './EntityKeyManager'
import { EntityManager, IEntityManager, IRequestClient } from './EntityManager'

export interface IEntityRepositoryClient extends IRequestClient, IKeyClient {
  transaction: () => IEntityRepositoryTransaction
}

export interface IEntityRepositoryTransaction extends IRequestClient {
  run: () => Promise<any>
  commit: () => Promise<any>
  rollback: () => Promise<any>
}

export interface IEntityRepository<T> {
  set: (id: string, data: T) => Promise<void>
  get: (id: string) => Promise<T | undefined>
  runInTransaction: <U>(fn: TransactionCallback<T, U>) => Promise<U>
}

export type TransactionCallback<T, U> = (
  manager: IEntityManager<T>
) => Promise<U> | U

export class EntityRepository<T = unknown> implements IEntityRepository<T> {
  private entityManager: IEntityManager<T>

  constructor(
    readonly entity: string,
    readonly client: IEntityRepositoryClient
  ) {
    const keyManager = new EntityKeyManager(entity, client)
    this.entityManager = new EntityManager(keyManager, client)
  }

  public async set(id: string, data: T): Promise<void> {
    return this.entityManager.set(id, data)
  }

  public async get(id: string): Promise<T | undefined> {
    return this.entityManager.get(id)
  }

  public async runInTransaction<U>(fn: TransactionCallback<T, U>): Promise<U> {
    const transaction = this.client.transaction()
    const keyManager = new EntityKeyManager(this.entity, this.client)
    const entityManager = new EntityManager<T>(keyManager, transaction)
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
