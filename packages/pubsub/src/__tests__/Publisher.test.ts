import { PubSub } from '@google-cloud/pubsub'
import { Schema, Type } from 'avsc'
import { createCallOptions } from '../createCallOptions'
import { DateType } from '../logical-types/DateType'
import { JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS, Publisher } from '../Publisher'
import {
  SCHEMA_DEFINITION_EXAMPLE, SCHEMA_DEFINITION_READER_OPTIONAL_ARRAY_EXAMPLE,
  SCHEMA_DEFINITION_WRITER_OPTIONAL_ARRAY_EXAMPLE,
} from './support/constants'
import {
  ConsoleLogger,
  getClientMock,
  getTopicMock, IMessageType,
  schemaMock,
} from './support/pubsubMock'


const topic = 'topic-name'
const topicMock = getTopicMock()
const clientMock = getClientMock({ topicMock })
const type = Type.forSchema(SCHEMA_DEFINITION_EXAMPLE as Schema, {logicalTypes: {'timestamp-micros': DateType}})
const schemas = {writer: SCHEMA_DEFINITION_EXAMPLE, reader: SCHEMA_DEFINITION_EXAMPLE}
const schemasWithArrays = {writer: SCHEMA_DEFINITION_WRITER_OPTIONAL_ARRAY_EXAMPLE,
  reader: SCHEMA_DEFINITION_READER_OPTIONAL_ARRAY_EXAMPLE}
const readerTypeWithArrays = Type.forSchema(SCHEMA_DEFINITION_READER_OPTIONAL_ARRAY_EXAMPLE as Schema,
  {logicalTypes: {'timestamp-micros': DateType}})


const DATE_WITH_UNSAFE_NUMBER_TIMESTAMP_IN_MICROS = new Date('3000-01-01T00:00:00.000Z')
const MAX_DATE_WITH_SAFE_NUMBER_TIMESTAMP_IN_MICROS = new Date('2255-06-05T23:47:34.740Z')

describe('Publisher', () => {
  let publisher: Publisher

  beforeEach(() => {
    process.env['GCLOUD_PROJECT'] = 'project'
    publisher = new Publisher(topic, clientMock as unknown as PubSub, schemas, new ConsoleLogger())
  })

  afterEach(() => {
    clientMock.topic.mockClear()
    topicMock.exists.mockReset()
    topicMock.create.mockReset()
    topicMock.publishMessage.mockReset()
    topicMock.getMetadata.mockReset()
    schemaMock.get.mockReset()
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


  describe('publishMsg on topic with schema', () => {
    const message = { first: 'one', second: 'two', createdAt: new Date() }
    const avroMessage = Buffer.from(type.toString(message))
    const metadata: Record<string, string> = {
      join_event: 'pubsub-test-event',
      join_generator_version: '1.0.0',
      join_generator_git_remote_origin_url: 'git@github.com:join-com/avro-join.git',
      join_schema_type: 'WRITER',
      join_avdl_schema_path_in_git_repo: 'src/test/resources/input.avdl',
      join_avdl_schema_git_remote_origin_url: 'git@github.com:join-com/data.git',
      join_avdl_schema_version: 'commit-hash',
      join_pubsub_lib_version: '0.0.0-development',

    }

    it('publishes avro json encoded object', async () => {
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{ 'schemaSettings': { 'schema': 'mock-schema' } }])
      await publisher.initialize()

      await publisher.publishMsg(message)

      expect(topicMock.publishMessage).toHaveBeenCalledWith({ data: avroMessage, attributes: metadata })
    })

    it('publishes avro json encoded object with attributes', async () => {
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{ 'schemaSettings': { 'schema': 'mock-schema' } }])
      await publisher.initialize()

      await publisher.publishMsg(message, { 'testKey': 'testValue' })

      expect(topicMock.publishMessage).toHaveBeenCalledWith({
        data: avroMessage, attributes: {
          ...metadata,
          'testKey': 'testValue',
        },
      })
    })

    it('publishes avro json with max allowed date value when date in micros overflows MAX_SAFE_INTEGER', async () => {
      publisher = new Publisher(topic, clientMock as unknown as PubSub, schemas, new ConsoleLogger())
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{ 'schemaSettings': { 'schema': 'mock-schema' } }])
      await publisher.initialize()

      const message = { first: 'one', createdAt: DATE_WITH_UNSAFE_NUMBER_TIMESTAMP_IN_MICROS }
      await publisher.publishMsg(message)

      const avroMessage = Buffer.from(type.toString(message))
      expect(topicMock.publishMessage).toHaveBeenCalledWith({ data: avroMessage, attributes: metadata })
      const decodedMessage = type.fromString(avroMessage.toString()) as IMessageType
      expect(decodedMessage.createdAt).toEqual(MAX_DATE_WITH_SAFE_NUMBER_TIMESTAMP_IN_MICROS)
    })

    it('publishes avro encoded messages with undefined array field in metadata', async () => {
      publisher = new Publisher(topic, clientMock as unknown as PubSub, schemasWithArrays, new ConsoleLogger())
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{ 'schemaSettings': { 'schema': 'mock-schema' } }])
      await publisher.initialize()
      const message = { first: 'one', tags: ['tag'] }

      await publisher.publishMsg(message)

      const avroMessage = Buffer.from(readerTypeWithArrays.toString(message))
      const messageMetadata = { ...metadata }
      messageMetadata[JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS] = 'languages'
      expect(topicMock.publishMessage).toHaveBeenCalledWith({ data: avroMessage, attributes: messageMetadata })
    })

    it('publishes avro encoded messages with undefined array field in metadata when array is null', async () => {
      publisher = new Publisher(topic, clientMock as unknown as PubSub, schemasWithArrays, new ConsoleLogger())
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{ 'schemaSettings': { 'schema': 'mock-schema' } }])
      await publisher.initialize()
      const message = { first: 'one', tags: ['tag'], languages: null }

      await publisher.publishMsg(message)

      const avroMessage = Buffer.from(readerTypeWithArrays.toString(message))
      const messageMetadata = { ...metadata }
      messageMetadata[JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS] = 'languages'
      expect(topicMock.publishMessage).toHaveBeenCalledWith({ data: avroMessage, attributes: messageMetadata })
    })

    it('publishes avro encoded messages with two undefined array field in metadata', async () => {
      publisher = new Publisher(topic, clientMock as unknown as PubSub, schemasWithArrays, new ConsoleLogger())
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{ 'schemaSettings': { 'schema': 'mock-schema' } }])
      await publisher.initialize()
      const message = { first: 'one' }

      await publisher.publishMsg(message)
      const avroMessage = Buffer.from(readerTypeWithArrays.toString(message))
      const messageMetadata = { ...metadata }
      messageMetadata[JOIN_UNDEFINED_OR_NULL_OPTIONAL_ARRAYS] = 'tags,languages'
      expect(topicMock.publishMessage).toHaveBeenCalledWith({ data: avroMessage, attributes: messageMetadata })
    })

    it('publishes avro encoded messages without undefined array field when all arrays are set', async () => {
      publisher = new Publisher(topic, clientMock as unknown as PubSub, schemasWithArrays, new ConsoleLogger())
      topicMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{ 'schemaSettings': { 'schema': 'mock-schema' } }])
      await publisher.initialize()
      const message = { first: 'one', tags: ['tag'], languages: ['en'] }

      await publisher.publishMsg(message)

      const avroMessage = Buffer.from(readerTypeWithArrays.toString(message))
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
