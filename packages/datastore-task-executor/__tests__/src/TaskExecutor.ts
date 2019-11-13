import { TaskExecutor } from '../../src/TaskExecutor'

const managerMock = {
  set: jest.fn(),
  get: jest.fn()
}

const repositoryMock = {
  set: jest.fn(),
  get: jest.fn(),
  runInTransaction: jest.fn(task => task(managerMock))
}

describe('TaskExecutor', () => {
  const taskId = 'task-id'

  let taskExecutor: TaskExecutor

  beforeEach(() => {
    taskExecutor = new TaskExecutor(repositoryMock as any)
  })

  afterEach(() => {
    repositoryMock.set.mockReset()
    repositoryMock.get.mockReset()
    managerMock.get.mockReset()
    managerMock.set.mockReset()
  })

  describe('execute', () => {
    const action = jest.fn()

    const processingStatus = { status: 'PROCESSING' }
    const completedStatus = { status: 'COMPLETED' }
    const failedStatus = { status: 'FAILED' }

    afterEach(() => {
      action.mockReset()
    })

    it('executes unless task was already registered', async () => {
      managerMock.get.mockResolvedValue(undefined)

      await taskExecutor.execute(taskId, action)

      expect(managerMock.get).toHaveBeenCalledWith(taskId)
      expect(managerMock.set).toHaveBeenCalledWith(taskId, processingStatus)

      expect(action).toHaveBeenCalled()
      expect(repositoryMock.set).toHaveBeenCalledWith(taskId, completedStatus)
    })

    it('executes if registered task has FAILED status', async () => {
      managerMock.get.mockResolvedValue(failedStatus)

      await taskExecutor.execute(taskId, action)

      expect(managerMock.get).toHaveBeenCalledWith(taskId)
      expect(managerMock.set).toHaveBeenCalledWith(taskId, processingStatus)

      expect(action).toHaveBeenCalled()
      expect(repositoryMock.set).toHaveBeenCalledWith(taskId, completedStatus)
    })

    it('skips execution if task has PROCESSING status', async () => {
      managerMock.get.mockResolvedValue(processingStatus)

      await taskExecutor.execute(taskId, action)

      expect(managerMock.get).toHaveBeenCalledWith(taskId)
      expect(managerMock.set).not.toHaveBeenCalled()
      expect(action).not.toHaveBeenCalled()
    })

    it('skips execution if task has FAILED status', async () => {
      managerMock.get.mockResolvedValue(processingStatus)

      await taskExecutor.execute(taskId, action)

      expect(managerMock.get).toHaveBeenCalledWith(taskId)
      expect(managerMock.set).not.toHaveBeenCalled()
      expect(action).not.toHaveBeenCalled()
    })

    describe('when action fails', () => {
      const error = new Error('Something wrong')

      beforeEach(() => {
        action.mockImplementation(() => {
          throw error
        })
      })

      it('saves task with FAILED status', async () => {
        expect.assertions(1)
        await taskExecutor.execute(taskId, action).catch(() => {
          expect(repositoryMock.set).toHaveBeenCalledWith(taskId, failedStatus)
        })
      })

      it('raises error', async () => {
        expect.assertions(1)
        await taskExecutor.execute(taskId, action).catch(e => {
          expect(e).toEqual(error)
        })
      })
    })
  })
})
