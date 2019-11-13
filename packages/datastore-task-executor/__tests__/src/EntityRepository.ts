import {
  EntityRepository,
  IEntityRepositoryClient
} from '../../src/EntityRepository'
import { Mock } from '../support/Mock'

const clientMock: Mock<IEntityRepositoryClient> = {
  key: jest.fn(),
  get: jest.fn(),
  save: jest.fn(),
  transaction: jest.fn()
}

describe('EntityRepository', () => {
  const entityName = 'entity-name'
  const recordId = 'record-id'
  const record = { state: 'PROCESSING' }

  let entityRepository: EntityRepository

  beforeEach(() => {
    entityRepository = new EntityRepository(entityName, clientMock)
  })

  afterEach(() => {
    clientMock.key.mockReset()
    clientMock.get.mockReset()
    clientMock.save.mockReset()
    clientMock.transaction.mockReset()
  })

  describe('get', () => {
    it('returns record', async () => {
      clientMock.get.mockResolvedValue([record])

      const result = await entityRepository.get(recordId)
      expect(result).toEqual(record)
      expect(clientMock.key).toHaveBeenCalledWith([entityName, recordId])
    })
  })

  describe('save', () => {
    it('saves record', async () => {
      await entityRepository.set(recordId, record)
      expect(clientMock.key).toHaveBeenCalledWith([entityName, recordId])
      expect(clientMock.save).toHaveBeenCalledWith(
        expect.objectContaining({ data: record })
      )
    })
  })

  describe('runInTransaction', () => {
    const transactionMock = {
      run: jest.fn(),
      commit: jest.fn(),
      rollback: jest.fn()
    }

    beforeEach(() => {
      clientMock.transaction.mockReturnValue(transactionMock)
    })

    afterEach(() => {
      transactionMock.run.mockReset()
      transactionMock.commit.mockReset()
      transactionMock.rollback.mockReset()
    })

    it('runs transaction', async () => {
      const entityManager = await entityRepository.runInTransaction(
        manager => manager
      )

      expect(entityManager.entity).toEqual(entityName)
      expect(entityManager.client).toEqual(transactionMock)
      expect(transactionMock.run).toHaveBeenCalled()
      expect(transactionMock.commit).toHaveBeenCalled()
    })

    it('rolls back if action fails', async () => {
      const error = new Error('Something wrong')

      expect.assertions(2)
      await entityRepository
        .runInTransaction(() => {
          throw error
        })
        .catch(e => {
          expect(transactionMock.rollback).toHaveBeenCalled()
          expect(e).toEqual(error)
        })
    })
  })
})
