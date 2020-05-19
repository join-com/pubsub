import { IEntityKeyManager } from '../../src/EntityKeyManager'
import { EntityManager, IRequestClient } from '../../src/EntityManager'
import { Mock } from '../support/Mock'

const keyManagerMock: Mock<IEntityKeyManager> = {
  getKey: jest.fn(),
}

const clientMock: Mock<IRequestClient> = {
  get: jest.fn(),
  save: jest.fn(),
}

describe('EntityManager', () => {
  const entityName = 'entity-name'
  const recordId = 'record-id'
  const record = { status: 'PROCESSING' }
  const key = { key: recordId }

  let entityManager: EntityManager

  beforeEach(() => {
    entityManager = new EntityManager(keyManagerMock, clientMock)
  })

  afterEach(() => {
    keyManagerMock.getKey.mockReset()
    clientMock.get.mockReset()
    clientMock.save.mockReset()
  })

  describe('get', () => {
    beforeEach(() => {
      clientMock.get.mockResolvedValue([record])
      keyManagerMock.getKey.mockReturnValue(key)
    })

    it('fetches record', async () => {
      const result = await entityManager.get(recordId)
      expect(keyManagerMock.getKey).toHaveBeenCalledWith(recordId)
      expect(clientMock.get).toHaveBeenCalledWith(key)
      expect(result).toEqual(record)
    })
  })

  describe('set', () => {
    beforeEach(() => {
      keyManagerMock.getKey.mockReturnValue(key)
    })

    it('saves record', async () => {
      await entityManager.set(recordId, record)
      expect(keyManagerMock.getKey).toHaveBeenCalledWith(recordId)
      expect(clientMock.save).toHaveBeenCalledWith({ key, data: record })
    })
  })
})
