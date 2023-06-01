import { PubSub } from '@google-cloud/pubsub'
import { Schema, Type } from 'avsc'
import { createCallOptions } from '../createCallOptions'
import { DateType } from '../logical-types/DateType'
import { Publisher } from '../Publisher'
import {
  ConsoleLogger,
  getClientMock,
  getTopicMock,
  SCHEMA_DEFINITION_EXAMPLE,
  SCHEMA_EXAMPLE,
  schemaMock,
} from './support/pubsubMock'


const topic = 'topic-name'
const topicMock = getTopicMock()
const clientMock = getClientMock({ topicMock })
const type = Type.forSchema(SCHEMA_DEFINITION_EXAMPLE as Schema, {logicalTypes: {'timestamp-micros': DateType}})
const processAbortSpy = jest.spyOn(process, 'abort')
const schemas = {writer: SCHEMA_DEFINITION_EXAMPLE, reader: SCHEMA_DEFINITION_EXAMPLE}


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
    topicMock.getMetadata.mockReset()
    schemaMock.get.mockReset()
    processAbortSpy.mockClear()

  })

  describe('initialize', () => {
    it('creates unless topic exists', async () => {
      topicMock.exists.mockResolvedValue([false])
      topicMock.getMetadata.mockResolvedValue([])

      await publisher.initialize()

      expect(topicMock.create).toHaveBeenCalledWith(createCallOptions)
      expect(clientMock.topic).toHaveBeenCalledWith(topic)
    })

    it('does not create if topic exists', async () => {
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([])

      await publisher.initialize()

      expect(topicMock.create).not.toHaveBeenCalled()
      expect(clientMock.topic).toHaveBeenCalledWith(topic)
    })
  })

  describe('publishMsg on topic without schema', () => {
    const message = { id: 1 }

    it('publishes json object', async () => {
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
    const avroMessage = Buffer.from(type.toString(message))
    const metadata = {
      join_event: 'data-company-affiliate-referral-created',
      join_generator_version: '1.0.0',
      join_generator_git_remote_origin_url: 'git@github.com:join-com/avro-join.git',
      join_schema_type: 'WRITER',
      join_avdl_schema_path_in_git_repo: 'src/test/resources/input.avdl',
      join_avdl_schema_git_remote_origin_url: 'git@github.com:join-com/data.git',
      join_avdl_schema_version: 'commit-hash',
      join_pubsub_lib_version: '0.0.0-development',

    }

    it('publishes avro json encoded object', async () => {
      publisher = new Publisher(topic, clientMock as unknown as PubSub, new ConsoleLogger(), schemas)
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{'schemaSettings': {'schema': 'mock-schema'}}])
      schemaMock.get.mockResolvedValue(SCHEMA_EXAMPLE)
      await publisher.initialize()

      await publisher.publishMsg(message)

      expect(topicMock.publishMessage).toHaveBeenCalledWith({ data: avroMessage, attributes: metadata })
    })
  })

  describe('flush', () => {
    it('flushes topic messages', async () => {
      await publisher.flush()

      expect(topicMock.flush).toHaveBeenCalled()
    })
  })
})
