import { EntityKeyManager, IKeyClient } from '../../src/EntityKeyManager'
import { Mock } from '../support/Mock'

const clientMock: Mock<IKeyClient> = {
  key: jest.fn(),
}

describe('EntityKeyManager', () => {
  const entityName = 'entity-name'

  let entityKeyManager: EntityKeyManager

  beforeEach(() => {
    entityKeyManager = new EntityKeyManager(entityName, clientMock)
  })

  afterEach(() => {
    clientMock.key.mockReset()
  })

  describe('getKey', () => {
    const recordId = 'record-id'
    const key = { key: recordId }

    beforeEach(() => {
      clientMock.key.mockResolvedValue(key)
    })

    it('returns key', async () => {
      const result = await entityKeyManager.getKey(recordId)
      expect(result).toEqual(key)
      expect(clientMock.key).toHaveBeenCalledWith([entityName, recordId])
    })
  })
})
