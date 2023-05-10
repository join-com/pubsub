import { PubSub } from '@google-cloud/pubsub'
import { Schema, Type } from 'avsc'
import { createCallOptions } from '../createCallOptions'
import { DateType } from '../logical-types/DateType'
import { Publisher } from '../Publisher'
import { ConsoleLogger, getClientMock, getTopicMock, SCHEMA_DEFINITION_EXAMPLE } from './support/pubsubMock'


const topic = 'topic-name'
const topicMock = getTopicMock()
const clientMock = getClientMock({ topicMock })
const type = Type.forSchema(SCHEMA_DEFINITION_EXAMPLE as Schema, {logicalTypes: {'timestamp-micros': DateType}})


describe('Publisher', () => {
  let publisher: Publisher

  beforeEach(() => {
    publisher = new Publisher(topic, clientMock as unknown as PubSub, new ConsoleLogger())
  })

  afterEach(() => {
    clientMock.topic.mockClear()
    clientMock.schema.mockClear()
    topicMock.exists.mockReset()
    topicMock.create.mockReset()
    topicMock.publishMessage.mockReset()
  })

  describe('initialize', () => {
    it('creates unless topic exists', async () => {
      topicMock.exists.mockResolvedValue([false])

      await publisher.initialize()

      expect(topicMock.create).toHaveBeenCalledWith(createCallOptions)
      expect(clientMock.topic).toHaveBeenCalledWith(topic)
    })

    it('does not create if topic exists', async () => {
      topicMock.exists.mockResolvedValue([true])

      await publisher.initialize()

      expect(topicMock.create).not.toHaveBeenCalled()
      expect(clientMock.topic).toHaveBeenCalledWith(topic)
    })

    it('gets schema when metadata is specified', async () => {
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{'schemaSettings': {'schema': 'mock-schema'}}])

      await publisher.initialize()

      expect(topicMock.create).not.toHaveBeenCalled()
      expect(clientMock.schema).toHaveBeenCalled()
    })

    it('gets validation topic schema when metadata is not specified', async () => {
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([])

      await publisher.initialize()

      expect(clientMock.schema).toHaveBeenCalled()
      expect(topicMock.create).not.toHaveBeenCalled()
    })
  })

  describe('publishMsg on topic without schema', () => {
    const message = { id: 1 }

    it('publishes json object with trace info', async () => {
      await publisher.publishMsg(message)

      expect(topicMock.publishMessage).toHaveBeenCalledWith({ json: message })
    })

    it('publishes json array', async () => {
      const array = [message, message]
      await publisher.publishMsg(array)

      expect(topicMock.publishMessage).toHaveBeenCalledWith({ json: array })
    })
  })

  describe('publishMsg on topic with schema', () => {
    const message = { first: 'one', second: 'two', createdAt: new Date() }
    const avroMessage = type.toBuffer(message)

    it('publishes avro binary encoded object with trace info', async () => {
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{'schemaSettings': {'schema': 'mock-schema'}}])
      await publisher.initialize()

      await publisher.publishMsg(message)

      expect(topicMock.publishMessage).toHaveBeenCalledWith({ data: avroMessage })
    })
  })

  describe('flush', () => {
    it('flushes topic messages', async () => {
      await publisher.flush()

      expect(topicMock.flush).toHaveBeenCalled()
    })
  })
})
