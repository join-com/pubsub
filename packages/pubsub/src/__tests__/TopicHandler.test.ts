import { PubSub, Topic } from '@google-cloud/pubsub'
import { TopicHandler } from '../TopicHandler'
import {
  ConsoleLogger,
  getClientMock,
  getTopicMock, SCHEMA_EXAMPLE,
  schemaMock,
} from './support/pubsubMock'

const topicMock = getTopicMock()
const clientMock = getClientMock({ topicMock })

describe('TopicHandler', () => {
  let topicHandler: TopicHandler

  beforeEach(() => {
    topicHandler = new TopicHandler(clientMock as unknown as PubSub, topicMock as unknown as Topic, new ConsoleLogger())
  })

  afterEach(() => {
    clientMock.topic.mockClear()
    topicMock.subscription.mockClear()
    topicMock.exists.mockReset()
    topicMock.create.mockReset()
  })

  describe('getSchemaType', () => {
    it('throws error when schema not returned from remote', async () => {
      schemaMock.get.mockResolvedValue({ definition: undefined })

      await expect(topicHandler.getSchemaType('some')).rejects.toThrow()
    })

    it('returns schema type when schema is returned from remote', async () => {
      schemaMock.get.mockResolvedValue(SCHEMA_EXAMPLE)

      const schemaType = await topicHandler.getSchemaType('some')
      expect(schemaType.schemaRevisionId).toEqual(SCHEMA_EXAMPLE.revisionId)
    })
  })

  describe('getTopicType', () => {
    it('returns undefined when schema is not specified on the topic', async () => {
      topicMock.getMetadata.mockResolvedValue([])
      const schemaType = await topicHandler.getSchemaTypeFromTopic()
      expect(schemaType).toBeUndefined()
    })


    it('returns topic type when schema is specified on the topic', async () => {
      schemaMock.get.mockResolvedValue(SCHEMA_EXAMPLE)
      topicMock.getMetadata.mockResolvedValue([{ 'schemaSettings': { 'schema': 'mock-schema' } }])

      const schemaType = await topicHandler.getSchemaType('some')
      expect(schemaType.schemaRevisionId).toEqual(SCHEMA_EXAMPLE.revisionId)
    })
  })
})
