import { EntityRepository } from './EntityRepository'

interface ITask {
  status: 'PROCESSING' | 'COMPLETED' | 'FAILED'
}

export class TaskExecutor {
  constructor(private repository: EntityRepository<ITask>) {}

  public async execute(taskId: string, action: () => Promise<void>) {
    const registered = await this.register(taskId)
    if (!registered) {
      return
    }

    try {
      await action()
      await this.repository.set(taskId, { status: 'COMPLETED' })
    } catch (error) {
      await this.repository.set(taskId, { status: 'FAILED' })
      throw error
    }
  }

  private register(taskId: string): Promise<boolean> {
    return this.repository.runInTransaction(async manager => {
      const record = await manager.get(taskId)
      if (record && record.status !== 'FAILED') {
        return false
      }

      await manager.set(taskId, { status: 'PROCESSING' })
      return true
    })
  }
}
