import {
  EntityManager,
  IEntityManager,
  IEntityManagerClient
} from './EntityManager'

export interface IEntityRepositoryClient extends IEntityManagerClient {
  transaction: () => IEntityRepositoryTransaction
}

export interface IEntityRepositoryTransaction extends IEntityManagerClient {
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
    this.entityManager = new EntityManager(entity, client)
  }

  public async set(id: string, data: T): Promise<void> {
    return this.entityManager.set(id, data)
  }

  public async get(id: string): Promise<T | undefined> {
    return this.entityManager.get(id)
  }

  public async runInTransaction<U>(fn: TransactionCallback<T, U>): Promise<U> {
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
