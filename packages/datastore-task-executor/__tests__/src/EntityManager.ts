import { EntityManager } from '../../src/EntityManager'

const clientMock = {
  key: jest.fn(),
  get: jest.fn(),
  save: jest.fn()
}

describe('EntityManager', () => {
  const entityName = 'entity-name'
  const recordId = 'record-id'
  const record = { status: 'PROCESSING' }
  const key = { key: recordId }

  let entityManager: EntityManager

  beforeEach(() => {
    entityManager = new EntityManager(entityName, clientMock as any)
    clientMock.key.mockReturnValue(key)
  })

  afterEach(() => {
    clientMock.key.mockReset()
    clientMock.get.mockReset()
    clientMock.save.mockReset()
  })

  describe('get', () => {
    beforeEach(() => {
      clientMock.get.mockResolvedValue([record])
    })

    it('fetches record', async () => {
      const result = await entityManager.get(recordId)
      expect(clientMock.key).toHaveBeenCalledWith([entityName, recordId])
      expect(clientMock.get).toHaveBeenCalledWith(key)
      expect(result).toEqual(record)
    })
  })

  describe('set', () => {
    it('saves record', async () => {
      await entityManager.set(recordId, record)
      expect(clientMock.key).toHaveBeenCalledWith([entityName, recordId])
      expect(clientMock.save).toHaveBeenCalledWith({ key, data: record })
    })
  })
})
