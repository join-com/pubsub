import { PubSub } from '@google-cloud/pubsub'
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { Schema, Type } from 'avsc'
import { createCallOptions } from '../createCallOptions'
import { DateType } from '../logical-types/DateType'
import { IParsedMessage, ISubscriptionOptions, Subscriber } from '../Subscriber'
import {
  ConsoleLogger,
  getClientMock,
  getIamMock,
  getMessageMock,
  getSubscriptionMock,
  getTopicMock,
  IMessageMock,
  SCHEMA_DEFINITION_EXAMPLE, SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE, schemaServiceClientMock,
} from './support/pubsubMock'

const topicName = 'topic-name'
const subscriptionName = 'subscription-name'

const iamSubscriptionMock = getIamMock()
const subscriptionMock = getSubscriptionMock({ iamMock: iamSubscriptionMock })
const iamTopicMock = getIamMock()
const topicMock = getTopicMock({ subscriptionMock, iamMock: iamTopicMock })
const clientMock = getClientMock({ topicMock })
const schemaClientMock = schemaServiceClientMock
const type = Type.forSchema(SCHEMA_DEFINITION_EXAMPLE as Schema, {logicalTypes: {'timestamp-micros': DateType}})
const typeWithPreserveNull = Type.forSchema(SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE as Schema, {logicalTypes: {'timestamp-micros': DateType}})
const flushPromises = () => new Promise(setImmediate);

const subscriptionOptions: ISubscriptionOptions = {
  ackDeadline: 10,
  allowExcessMessages: true,
  maxMessages: 5,
  maxStreams: 1,
  minBackoffSeconds: 1,
  maxBackoffSeconds: 10,
}

describe('Subscriber', () => {
  let subscriber: Subscriber

  beforeEach(() => {
    subscriber = new Subscriber({ topicName, subscriptionName, subscriptionOptions }, clientMock as unknown as PubSub,
      schemaClientMock as unknown as SchemaServiceClient, new ConsoleLogger())
    process.env['GCLOUD_PROJECT'] = 'project'
  })

  afterEach(() => {
    clientMock.topic.mockClear()
    clientMock.schema.mockClear()
    topicMock.subscription.mockClear()
    topicMock.exists.mockReset()
    topicMock.create.mockReset()
    subscriptionMock.exists.mockReset()
    subscriptionMock.create.mockReset()
    subscriptionMock.setMetadata.mockReset()
    iamTopicMock.setPolicy.mockReset()
    iamSubscriptionMock.setPolicy.mockReset()
    schemaClientMock.getSchema.mockReset()
  })

  describe('initialize', () => {
    it('creates topic unless exists', async () => {
      topicMock.exists.mockResolvedValue([false])
      subscriptionMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([])

      await subscriber.initialize()

      expect(topicMock.create).toHaveBeenCalledTimes(1)
      expect(topicMock.create).toHaveBeenCalledWith(createCallOptions)
      expect(clientMock.topic).toHaveBeenCalledWith(topicName)
    })

    it('does not create topic if exists', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])

      await subscriber.initialize()

      expect(topicMock.create).not.toHaveBeenCalled()
      expect(clientMock.topic).toHaveBeenCalledWith(topicName)
    })

    it('creates subscription unless exists', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([false])

      await subscriber.initialize()

      expect(subscriptionMock.create).toHaveBeenCalledWith({
        deadLetterPolicy: null,
        retryPolicy: {
          minimumBackoff: { seconds: subscriptionOptions.minBackoffSeconds },
          maximumBackoff: { seconds: subscriptionOptions.maxBackoffSeconds },
        },
        gaxOpts: createCallOptions,
      })

      expect(topicMock.subscription).toHaveBeenCalledWith(subscriptionName, {
        ackDeadline: subscriptionOptions.ackDeadline,
        flowControl: {
          allowExcessMessages: subscriptionOptions.allowExcessMessages,
          maxMessages: subscriptionOptions.maxMessages,
        },
        streamingOptions: {
          maxStreams: subscriptionOptions.maxStreams,
        },
      })

      expect(subscriptionMock.setMetadata).not.toHaveBeenCalled()
    })

    it('updates metadata if subscription exists', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).toHaveBeenCalledWith({
        deadLetterPolicy: null,
        retryPolicy: {
          minimumBackoff: { seconds: subscriptionOptions.minBackoffSeconds },
          maximumBackoff: { seconds: subscriptionOptions.maxBackoffSeconds },
        },
      })
    })

    it('resets retry policy unless backoff values provided', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])

      subscriber = new Subscriber({ topicName, subscriptionName }, clientMock as unknown as PubSub,
        schemaClientMock as unknown as SchemaServiceClient)

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).toHaveBeenCalledWith({
        deadLetterPolicy: null,
        retryPolicy: {},
      })
    })

    describe('dead letter policy', () => {
      const deadLetterTopicName = 'subscription-name-unack'
      const deadLetterSubscriptionName = 'subscription-name-unack'

      const deadLetterOptions: ISubscriptionOptions = {
        ...subscriptionOptions,
        maxDeliveryAttempts: 123,
        gcloudProject: {
          name: 'gcloudProjectName',
          id: 123456789,
        },
      }

      beforeEach(() => {
        subscriber = new Subscriber(
          { topicName, subscriptionName, subscriptionOptions: deadLetterOptions },
          clientMock as unknown as PubSub, schemaClientMock as unknown as SchemaServiceClient
        )
      })

      describe('deadLetterTopic initialization', () => {
        beforeEach(() => {
          subscriptionMock.exists.mockResolvedValue([true])
        })

        it('creates deadLetterTopic unless exists', async () => {
          topicMock.exists.mockResolvedValue([false])

          await subscriber.initialize()

          expect(topicMock.create).toHaveBeenCalledTimes(2)
          expect(clientMock.topic).toHaveBeenLastCalledWith(deadLetterTopicName)
        })

        it('adds publisher role to pubsub service account', async () => {
          topicMock.exists.mockResolvedValue([false])

          await subscriber.initialize()

          expect(iamTopicMock.setPolicy).toHaveBeenCalledWith({
            bindings: [
              {
                members: ['serviceAccount:service-123456789@gcp-sa-pubsub.iam.gserviceaccount.com'],
                role: 'roles/pubsub.publisher',
              },
            ],
          })
        })

        it('does not create deadLetterTopic if exists', async () => {
          topicMock.exists.mockResolvedValue([true])

          await subscriber.initialize()

          expect(topicMock.create).not.toHaveBeenCalled()
          expect(clientMock.topic).toHaveBeenLastCalledWith(deadLetterTopicName)
        })

        it('does not create deadLetterTopic if not necessary', async () => {
          topicMock.exists.mockResolvedValue([false])
          const emptyOptions = {}
          const optionlessSubscriber = new Subscriber(
            { topicName, subscriptionName, subscriptionOptions: emptyOptions },
            clientMock as unknown as PubSub, schemaClientMock as unknown as SchemaServiceClient
          )

          await optionlessSubscriber.initialize()

          expect(topicMock.create).toHaveBeenCalledTimes(1)
          expect(clientMock.topic).toHaveBeenLastCalledWith(topicName)
          expect(iamTopicMock.setPolicy).not.toHaveBeenCalled()
        })
      })

      describe('deadLetterSubscription initialization', () => {
        beforeEach(() => {
          topicMock.exists.mockResolvedValue([true])
        })

        it('creates deadLetterSubscription unless exists', async () => {
          subscriptionMock.exists.mockResolvedValue([false])

          await subscriber.initialize()

          expect(subscriptionMock.create).toHaveBeenCalledTimes(2)
          expect(subscriptionMock.create).toHaveBeenLastCalledWith({
            gaxOpts: createCallOptions,
          })
          expect(topicMock.subscription).toHaveBeenLastCalledWith(deadLetterSubscriptionName)
        })

        it('adds subscriber role to pubsub service account', async () => {
          subscriptionMock.exists.mockResolvedValue([false])

          await subscriber.initialize()

          expect(iamSubscriptionMock.setPolicy).toHaveBeenCalledWith({
            bindings: [
              {
                members: ['serviceAccount:service-123456789@gcp-sa-pubsub.iam.gserviceaccount.com'],
                role: 'roles/pubsub.subscriber',
              },
            ],
          })
        })

        it('does not create deadLetterSubscription if exists', async () => {
          subscriptionMock.exists.mockResolvedValue([true])

          await subscriber.initialize()

          expect(subscriptionMock.create).not.toHaveBeenCalled()
          expect(topicMock.subscription).toHaveBeenLastCalledWith(deadLetterSubscriptionName)
        })

        it('does not create deadLetterSubscription if not necessary', async () => {
          subscriptionMock.exists.mockResolvedValue([false])
          const emptyOptions = {}
          const optionlessSubscriber = new Subscriber(
            { topicName, subscriptionName, subscriptionOptions: emptyOptions },
            clientMock as unknown as PubSub, schemaClientMock as unknown as SchemaServiceClient
          )

          await optionlessSubscriber.initialize()

          expect(subscriptionMock.create).toHaveBeenCalledTimes(1)
          expect(subscriptionMock.create).toHaveBeenCalledWith({
            deadLetterPolicy: null,
            retryPolicy: {},
            gaxOpts: createCallOptions,
          })
          expect(topicMock.subscription).not.toHaveBeenCalledWith(deadLetterSubscriptionName, expect.anything())
          expect(iamSubscriptionMock.setPolicy).not.toHaveBeenCalled()
        })

        it('creates subscription with deadLetterTopic reference', async () => {
          subscriptionMock.exists.mockResolvedValue([false])

          await subscriber.initialize()

          expect(subscriptionMock.create).toHaveBeenCalledWith({
            retryPolicy: {
              minimumBackoff: { seconds: subscriptionOptions.minBackoffSeconds },
              maximumBackoff: { seconds: subscriptionOptions.maxBackoffSeconds },
            },
            deadLetterPolicy: {
              maxDeliveryAttempts: 123,
              deadLetterTopic: 'projects/gcloudProjectName/topics/subscription-name-unack',
            },
            gaxOpts: createCallOptions,
          })
        })
      })
    })
  })

  describe('start', () => {
    const data = { id: 1, createdAt: new Date() }
    const avroData = { first: 'one', second: 'two', third: undefined,
      createdAt: new Date('Thu Nov 05 2015 11:38:05 GMT-0800 (PST)')}
    const avroDataPreserveNullMessageFromAvro = { first: 'one', second: 'two', third: undefined,
      createdAt: new Date('Thu Nov 05 2015 11:38:05 GMT-0800 (PST)'),
      now: { id: null, firstName: null }}

    let messageMock: IMessageMock

    beforeEach(() => {
      messageMock = getMessageMock(data)
    })

    it('receives parsed data', async () => {
      let parsedMessage: IParsedMessage<unknown> | undefined
      subscriber.start(msg => {
        parsedMessage = msg
        return Promise.resolve()
      })

      await subscriptionMock.receiveMessage(messageMock)

      expect(parsedMessage?.dataParsed).toEqual(data)
    })

    it('receives avro parsed data', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{'schemaSettings': {'schema': 'mock-schema'}}])
      schemaClientMock.getSchema.mockResolvedValue([{definition: JSON.stringify(SCHEMA_DEFINITION_EXAMPLE)}])

      await subscriber.initialize()

      messageMock.data = Buffer.from(type.toString(avroData))
      messageMock.attributes = {'googclient_schemarevisionid': 'example'}

      let parsedMessage: IParsedMessage<unknown> | undefined
      subscriber.start(msg => {
        parsedMessage = msg
        return Promise.resolve()
      })

      await subscriptionMock.receiveMessage(messageMock)
      await flushPromises()
      expect(parsedMessage?.dataParsed).toEqual(avroData)
    })

    it('receives avro parsed data with null preserve fields', async () => {
      topicMock.exists.mockResolvedValue([false])
      subscriptionMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{'schemaSettings': {'schema': 'mock-schema'}}])
      schemaClientMock.getSchema.mockResolvedValue([{definition: JSON.stringify(SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE)}])

      await subscriber.initialize()

      messageMock.data = Buffer.from(typeWithPreserveNull.toString(avroDataPreserveNullMessageFromAvro))
      messageMock.attributes = {'googclient_schemarevisionid': 'example', 'join_preserve_null': 'now'}

      let parsedMessage: IParsedMessage<unknown> | undefined
      subscriber.start(msg => {
        parsedMessage = msg
        return Promise.resolve()
      })

      await subscriptionMock.receiveMessage(messageMock)
      await flushPromises()
      expect(parsedMessage?.dataParsed).toEqual(avroDataPreserveNullMessageFromAvro)
    })

    it('processes avro encoded data without assigned schema from gcloud', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([{'schemaSettings': {'schema': 'mock-schema'}}])
      schemaClientMock.listSchemaRevisions.mockResolvedValue([[{revisionId: 'revision', definition: JSON.stringify(SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE)}]])

      await subscriber.initialize()

      messageMock.data = Buffer.from(type.toString(avroData))
      messageMock.attributes = {'join_avdl_schema_version': 'example'}

      let parsedMessage: IParsedMessage<unknown> | undefined
      subscriber.start(msg => {
        parsedMessage = msg
        return Promise.resolve()
      })

      await subscriptionMock.receiveMessage(messageMock)
      await flushPromises()
      expect(parsedMessage?.dataParsed).toEqual(avroData)
    })

    it('processes avro encoded data with latest schema when schema from message can not be found', async () => {
        topicMock.exists.mockResolvedValue([true])
        subscriptionMock.exists.mockResolvedValue([true])
        topicMock.getMetadata.mockResolvedValue([{'schemaSettings': {'schema': 'mock-schema'}}])

        schemaClientMock.getSchema.mockRejectedValue(new Error('NOT_FOUND'))
        schemaClientMock.listSchemaRevisions.mockResolvedValue([[{revisionId: 'revision', definition: JSON.stringify(SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE)}]])
        await subscriber.initialize()

        messageMock.data = Buffer.from(type.toString(avroData))
        messageMock.attributes = {'googclient_schemarevisionid': 'example'}

        let parsedMessage: IParsedMessage<unknown> | undefined
        subscriber.start(msg => {
          parsedMessage = msg
          return Promise.resolve()
        })

      await subscriptionMock.receiveMessage(messageMock)
      await flushPromises()
      expect(parsedMessage?.dataParsed).toEqual(avroData)
    })

    it('unacknowledges message if processing fails', async () => {
      subscriber.start(() => Promise.reject('Something wrong'))

      await subscriptionMock.receiveMessage(messageMock)

      expect(messageMock.nack).toHaveBeenCalled()
    })
  })

  describe('stop', () => {
    it('closes subscription', async () => {
      await subscriber.stop()

      expect(subscriptionMock.close).toHaveBeenCalled()
    })
  })
})
