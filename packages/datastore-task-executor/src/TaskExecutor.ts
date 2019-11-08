import { logger } from '@join-com/gcloud-logger-trace'
import { EntityRepository } from './EntityRepository'

interface ITask {
  status: 'PROCESSING' | 'COMPLETED' | 'FAILED'
}

export class TaskExecutor {
  constructor(readonly repository: EntityRepository<ITask>) {}

  public async execute(taskId: string, action: () => Promise<void>) {
    logger.debug('Task execution starts', { taskId })
    const registered = await this.register(taskId)
    if (!registered) {
      logger.debug('Task execution skipped', { taskId })
      return
    }

    try {
      await action()
      logger.debug('Action completed')
      await this.repository.set(taskId, { status: 'COMPLETED' })
      logger.debug('Task execution completed')
    } catch (error) {
      logger.debug('Action execution failed', error)
      await this.repository.set(taskId, { status: 'FAILED' })
      logger.debug('Task execution failed')
      throw error
    }
  }

  private register(taskId: string): Promise<boolean> {
    return this.repository.runInTransaction(async manager => {
      const record = await manager.get(taskId)
      if (record && record.status !== 'FAILED') {
        logger.debug('Task was already processed', record)
        return false
      }

      await manager.set(taskId, { status: 'PROCESSING' })
      return true
    })
  }
}
