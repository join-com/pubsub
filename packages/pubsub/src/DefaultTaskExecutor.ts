export interface ITaskExecutor {
  execute: (taskId: string, action: () => Promise<void>) => Promise<void>
}

export class DefaultTaskExecutor implements ITaskExecutor {
  public execute(_: string, action: () => Promise<void>) {
    return action()
  }
}
