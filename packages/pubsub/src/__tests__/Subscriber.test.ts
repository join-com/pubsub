import { PubSub } from '@google-cloud/pubsub'
import { SchemaServiceClient, SubscriberClient } from '@google-cloud/pubsub/build/src/v1'
import { Schema, Type } from 'avsc'
import { createCallOptions } from '../createCallOptions'
import { ILogger } from '../ILogger'
import { DateType } from '../logical-types/DateType'
import { IParsedMessage, ISubscriptionOptions, Subscriber } from '../Subscriber'
import {
  SCHEMA_DEFINITION_EXAMPLE,
  SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE,
  SCHEMA_DEFINITION_READER_OPTIONAL_ARRAY_EXAMPLE,
} from './support/constants'
import {
  ConsoleLogger,
  getClientMock,
  getIamMock,
  getMessageMock,
  getSubscriptionMock,
  getTopicMock,
  schemaServiceClientMock,
} from './support/pubsubMock'

const topicName = 'topic-name'
const subscriptionName = 'subscription-name'

const iamSubscriptionMock = getIamMock()
const subscriptionMock = getSubscriptionMock({ iamMock: iamSubscriptionMock })
const iamTopicMock = getIamMock()
const topicMock = getTopicMock({ subscriptionMock, iamMock: iamTopicMock })
const clientMock = getClientMock({ topicMock })
const schemaClientMock = schemaServiceClientMock
const type = Type.forSchema(SCHEMA_DEFINITION_EXAMPLE as Schema, { logicalTypes: { 'timestamp-micros': DateType } })
const readerTypeWithArrays = Type.forSchema(SCHEMA_DEFINITION_READER_OPTIONAL_ARRAY_EXAMPLE as Schema, {
  logicalTypes: { 'timestamp-micros': DateType },
})
const typeWithPreserveNull = Type.forSchema(SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE as Schema, {
  logicalTypes: { 'timestamp-micros': DateType },
})
const flushPromises = () => new Promise(setImmediate)

const subscriptionOptions: ISubscriptionOptions = {
  ackDeadline: 10,
  allowExcessMessages: true,
  maxMessages: 5,
  maxStreams: 1,
  minBackoffSeconds: 1,
  maxBackoffSeconds: 10,
  labels: { testKey: 'testValue' },
}

const loggerMock = jest.mocked<ILogger>({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
})

describe('Subscriber', () => {
  let subscriber: Subscriber

  beforeEach(() => {
    process.env['GCLOUD_PROJECT'] = 'project'
    process.env['PUBSUB_SUBSCRIPTION_TOPIC_INIT'] = undefined
    subscriber = new Subscriber(
      { topicName, subscriptionName, subscriptionOptions },
      clientMock as unknown as PubSub,
      schemaClientMock as unknown as SchemaServiceClient,
      undefined as unknown as SubscriberClient,
      new ConsoleLogger(),
    )
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
    subscriptionMock.getMetadata.mockReset()
    subscriptionMock.getMetadata.mockReset()
    iamTopicMock.setPolicy.mockReset()
    iamSubscriptionMock.setPolicy.mockReset()
    schemaClientMock.getSchema.mockReset()
    loggerMock.info.mockReset()
    loggerMock.warn.mockReset()
    loggerMock.error.mockReset()
  })

  describe('initialize', () => {
    it('creates topic unless exists if PUBSUB_SUBSCRIPTION_TOPIC_INIT equals true', async () => {
      process.env['PUBSUB_SUBSCRIPTION_TOPIC_INIT'] = 'true'
      topicMock.exists.mockResolvedValue([false])
      subscriptionMock.exists.mockResolvedValue([true])
      topicMock.getMetadata.mockResolvedValue([])
      subscriptionMock.getMetadata.mockResolvedValue([{}])

      await subscriber.initialize()

      expect(topicMock.create).toHaveBeenCalledTimes(1)
      expect(topicMock.create).toHaveBeenCalledWith(createCallOptions)
      expect(clientMock.topic).toHaveBeenCalledWith(topicName)
    })

    it('does not create topic if exists', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([{}])

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
        labels: subscriptionOptions.labels,
        gaxOpts: createCallOptions,
        filter: '',
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

    it('creates subscription with filter', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([false])
      const subscriberWithFilter = new Subscriber(
        {
          topicName,
          subscriptionName,
          subscriptionOptions: {
            ...subscriptionOptions,
            filter: 'attributes.testKey="testValue"',
          },
        },
        clientMock as unknown as PubSub,
        schemaClientMock as unknown as SchemaServiceClient,
        undefined as unknown as SubscriberClient,
        new ConsoleLogger(),
      )

      await subscriberWithFilter.initialize()

      expect(subscriptionMock.create).toHaveBeenCalledWith({
        deadLetterPolicy: null,
        retryPolicy: {
          minimumBackoff: { seconds: subscriptionOptions.minBackoffSeconds },
          maximumBackoff: { seconds: subscriptionOptions.maxBackoffSeconds },
        },
        labels: subscriptionOptions.labels,
        gaxOpts: createCallOptions,
        filter: 'attributes.testKey="testValue"',
      })
    })

    it('throws error if filter has changed', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([
        {
          filter: 'attributes.testKey="currentValue"',
        },
      ])

      const subscriberWithFilter = new Subscriber(
        {
          topicName,
          subscriptionName,
          subscriptionOptions: {
            ...subscriptionOptions,
            filter: 'attributes.testKey="newValue"',
          },
        },
        clientMock as unknown as PubSub,
        schemaClientMock as unknown as SchemaServiceClient,
        undefined as unknown as SubscriberClient,
        loggerMock,
      )
      const processAbortSpy = jest.spyOn(process, 'abort').mockImplementation()

      await subscriberWithFilter.initialize()

      expect(loggerMock.error).toHaveBeenCalledWith(
        'PubSub: Failed to initialize subscriber subscription-name',
        new Error(
          "PubSub: Subscriptions filters are immutable, they can't be changed, subscription: subscription-name, " +
          'currentFilter: attributes.testKey="currentValue", newFilter: attributes.testKey="newValue"',
        ),
      )
      processAbortSpy.mockClear()
    })

    it('does not throw when deployed again without filter', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      // google cloud returns filter as empty string when no filter is set
      subscriptionMock.getMetadata.mockResolvedValue([
        {
          filter: '',
        },
      ])

      await subscriber.initialize()

      expect(loggerMock.error).not.toHaveBeenCalled()
    })

    it('updates metadata if backoff has changed', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([
        {
          retryPolicy: {
            minimumBackoff: { seconds: '45' },
            maximumBackoff: { seconds: String(subscriptionOptions.maxBackoffSeconds) },
          },
        },
      ])

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).toHaveBeenCalledWith({
        deadLetterPolicy: null,
        retryPolicy: {
          minimumBackoff: { seconds: subscriptionOptions.minBackoffSeconds },
          maximumBackoff: { seconds: subscriptionOptions.maxBackoffSeconds },
        },
        labels: subscriptionOptions.labels,
      })
    })

    it('does not update metadata if subscription exists and did not change', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([
        {
          retryPolicy: {
            minimumBackoff: { seconds: String(subscriptionOptions.minBackoffSeconds) },
            maximumBackoff: { seconds: String(subscriptionOptions.maxBackoffSeconds) },
          },
          labels: subscriptionOptions.labels,
        },
      ])

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).not.toHaveBeenCalled()
    })

    it('does not update retry policy if no values provided', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([{}])

      subscriber = new Subscriber(
        { topicName, subscriptionName },
        clientMock as unknown as PubSub,
        schemaClientMock as unknown as SchemaServiceClient,
        undefined as unknown as SubscriberClient,
      )

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).not.toHaveBeenCalled()
    })

    it('updates labels if labels have changed', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([
        {
          labels: { testKey: 'oldValue' },
        },
      ])

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).toHaveBeenCalledWith({
        deadLetterPolicy: null,
        retryPolicy: {
          minimumBackoff: { seconds: subscriptionOptions.minBackoffSeconds },
          maximumBackoff: { seconds: subscriptionOptions.maxBackoffSeconds },
        },
        labels: subscriptionOptions.labels,
      })
    })

    it('updates labels if labels were not empty and now are set to empty object', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([
        {
          retryPolicy: {
            minimumBackoff: { seconds: String(subscriptionOptions.minBackoffSeconds) },
            maximumBackoff: { seconds: String(subscriptionOptions.maxBackoffSeconds) },
          },
          labels: { testKey: 'testValue' },
        },
      ])
      const optionsWithNullLabels = { ...subscriptionOptions, labels: {} }
      subscriber = new Subscriber(
        { topicName, subscriptionName, subscriptionOptions: optionsWithNullLabels },
        clientMock as unknown as PubSub,
        schemaClientMock as unknown as SchemaServiceClient,
        undefined as unknown as SubscriberClient,
        new ConsoleLogger(),
      )

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).toHaveBeenCalledWith({
        deadLetterPolicy: null,
        retryPolicy: {
          minimumBackoff: { seconds: subscriptionOptions.minBackoffSeconds },
          maximumBackoff: { seconds: subscriptionOptions.maxBackoffSeconds },
        },
        labels: {},
      })
    })

    it('does not update labels if labels order in object changed', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([
        {
          retryPolicy: {
            minimumBackoff: { seconds: String(subscriptionOptions.minBackoffSeconds) },
            maximumBackoff: { seconds: String(subscriptionOptions.maxBackoffSeconds) },
          },
          labels: { testKey: 'testValue', testKey2: 'testValue2' },
        },
      ])
      const optionsWithNullLabels = { ...subscriptionOptions, labels: { testKey2: 'testValue2', testKey: 'testValue' } }
      subscriber = new Subscriber(
        { topicName, subscriptionName, subscriptionOptions: optionsWithNullLabels },
        clientMock as unknown as PubSub,
        schemaClientMock as unknown as SchemaServiceClient,
        undefined as unknown as SubscriberClient,
        new ConsoleLogger(),
      )

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).not.toHaveBeenCalled()
    })

    it('does not update metadata if dead letter policy did not change', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      const returnedFromGcloudDeadLetterPolicy = {
        maxDeliveryAttempts: 123,
        deadLetterTopic: 'projects/gcloudProjectName/topics/subscription-name-unack',
      }
      const topicSubscriptionMetadata = {
        retryPolicy: {
          minimumBackoff: { seconds: String(subscriptionOptions.minBackoffSeconds) },
          maximumBackoff: { seconds: String(subscriptionOptions.maxBackoffSeconds) },
        },
        labels: { testKey: 'testValue' },
        deadLetterPolicy: returnedFromGcloudDeadLetterPolicy,
      }
      subscriptionMock.getMetadata.mockResolvedValueOnce([topicSubscriptionMetadata])
      const deadLetterTopicSubscriptionMetadata = {
        labels: { testKey: 'testValue' },
      }
      subscriptionMock.getMetadata.mockResolvedValueOnce([deadLetterTopicSubscriptionMetadata])
      const optionsWithSameDeadLetterPolicy: ISubscriptionOptions = {
        ...subscriptionOptions,
        maxDeliveryAttempts: 123,
        gcloudProject: {
          name: 'gcloudProjectName',
          id: 123456789,
        },
      }
      subscriber = new Subscriber(
        { topicName, subscriptionName, subscriptionOptions: optionsWithSameDeadLetterPolicy },
        clientMock as unknown as PubSub,
        schemaClientMock as unknown as SchemaServiceClient,
        undefined as unknown as SubscriberClient,
        new ConsoleLogger(),
      )

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).not.toHaveBeenCalled()
    })

    it('updates metadata if dead letter policy changed', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      const returnedFromGcloudDeadLetterPolicy = {
        maxDeliveryAttempts: 123,
        deadLetterTopic: 'projects/gcloudProjectName/topics/subscription-name-unack',
      }
      subscriptionMock.getMetadata.mockResolvedValue([
        {
          retryPolicy: {
            minimumBackoff: { seconds: String(subscriptionOptions.minBackoffSeconds) },
            maximumBackoff: { seconds: String(subscriptionOptions.maxBackoffSeconds) },
          },
          labels: { testKey: 'testValue' },
          deadLetterPolicy: returnedFromGcloudDeadLetterPolicy
        },
      ])
      const optionsWithChangedDeadLetterPolicy: ISubscriptionOptions = { ...subscriptionOptions,
        maxDeliveryAttempts: 10,
        gcloudProject: {
          name: 'gcloudProjectName',
          id: 123456789,
        },
      }
      subscriber = new Subscriber(
        { topicName, subscriptionName, subscriptionOptions: optionsWithChangedDeadLetterPolicy },
        clientMock as unknown as PubSub,
        schemaClientMock as unknown as SchemaServiceClient,
        undefined as unknown as SubscriberClient,
        new ConsoleLogger(),
      )

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).toHaveBeenCalledWith({
        deadLetterPolicy: {
          maxDeliveryAttempts: 10,
          deadLetterTopic: 'projects/gcloudProjectName/topics/subscription-name-unack',
        },
        retryPolicy: {
          minimumBackoff: { seconds: subscriptionOptions.minBackoffSeconds },
          maximumBackoff: { seconds: subscriptionOptions.maxBackoffSeconds },
        },
        labels: { testKey: 'testValue' },
      })
    })

    it('updates metadata if dead letter policy disabled', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      const deadLetterPolicy = {
        maxDeliveryAttempts: 123,
        deadLetterTopic: 'projects/gcloudProjectName/topics/subscription-name-unack',
      }
      subscriptionMock.getMetadata.mockResolvedValue([
        {
          retryPolicy: {
            minimumBackoff: { seconds: String(subscriptionOptions.minBackoffSeconds) },
            maximumBackoff: { seconds: String(subscriptionOptions.maxBackoffSeconds) },
          },
          labels: { testKey: 'testValue' },
          deadLetterPolicy
        },
      ])
      subscriber = new Subscriber(
        { topicName, subscriptionName, subscriptionOptions },
        clientMock as unknown as PubSub,
        schemaClientMock as unknown as SchemaServiceClient,
        undefined as unknown as SubscriberClient,
        new ConsoleLogger(),
      )

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(subscriptionMock.setMetadata).toHaveBeenCalledWith({
        deadLetterPolicy: null,
        retryPolicy: {
          minimumBackoff: { seconds: subscriptionOptions.minBackoffSeconds },
          maximumBackoff: { seconds: subscriptionOptions.maxBackoffSeconds },
        },
        labels: { testKey: 'testValue' },
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
          clientMock as unknown as PubSub,
          schemaClientMock as unknown as SchemaServiceClient,
          undefined as unknown as SubscriberClient,
        )
      })

      describe('deadLetterTopic initialization', () => {
        beforeEach(() => {
          subscriptionMock.exists.mockResolvedValue([true])
          subscriptionMock.getMetadata.mockResolvedValue([{}])
        })

        it('creates deadLetterTopic unless exists', async () => {
          topicMock.exists.mockResolvedValue([false])

          await subscriber.initialize()

          expect(topicMock.create).toHaveBeenCalledTimes(1)
          expect(clientMock.topic).toHaveBeenCalledWith(deadLetterTopicName)
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
            clientMock as unknown as PubSub,
            schemaClientMock as unknown as SchemaServiceClient,
            undefined as unknown as SubscriberClient,
          )

          await optionlessSubscriber.initialize()

          expect(topicMock.create).not.toHaveBeenCalled()
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
            deadLetterPolicy: null,
            retryPolicy: {},
            labels: { testKey: 'testValue' },
            filter: '',
            gaxOpts: createCallOptions,
          })
          expect(topicMock.subscription).toHaveBeenLastCalledWith(deadLetterSubscriptionName)
        })

        it('updates labels on dead letter subscription if changed', async () => {
          subscriptionMock.exists.mockResolvedValue([true])
          const deadLetterPolicy = {
            maxDeliveryAttempts: 123,
            deadLetterTopic: 'projects/gcloudProjectName/topics/subscription-name-unack',
          }
          const topicSubscriptionMetadata = {
            retryPolicy: {
              minimumBackoff: { seconds: String(subscriptionOptions.minBackoffSeconds) },
              maximumBackoff: { seconds: String(subscriptionOptions.maxBackoffSeconds) },
            },
            labels: { testKey: 'testValue' },
            deadLetterPolicy
          }
          subscriptionMock.getMetadata.mockResolvedValueOnce([topicSubscriptionMetadata])
          const deadLetterTopicSubscriptionMetadata = {
            labels: { },
          }
          subscriptionMock.getMetadata.mockResolvedValueOnce([deadLetterTopicSubscriptionMetadata])

          await subscriber.initialize()

          expect(subscriptionMock.setMetadata).toHaveBeenLastCalledWith({
            deadLetterPolicy: null,
            retryPolicy: {},
            labels: { testKey: 'testValue' },
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
          subscriptionMock.getMetadata.mockResolvedValue([{}])

          await subscriber.initialize()

          expect(subscriptionMock.create).not.toHaveBeenCalled()
          expect(topicMock.subscription).toHaveBeenLastCalledWith(deadLetterSubscriptionName)
        })

        it('does not create deadLetterSubscription if not necessary', async () => {
          subscriptionMock.exists.mockResolvedValue([false])
          const emptyOptions = {}
          const optionlessSubscriber = new Subscriber(
            { topicName, subscriptionName, subscriptionOptions: emptyOptions },
            clientMock as unknown as PubSub,
            schemaClientMock as unknown as SchemaServiceClient,
            undefined as unknown as SubscriberClient,
          )

          await optionlessSubscriber.initialize()

          expect(subscriptionMock.create).toHaveBeenCalledTimes(1)
          expect(subscriptionMock.create).toHaveBeenCalledWith({
            deadLetterPolicy: null,
            retryPolicy: {},
            gaxOpts: createCallOptions,
            filter: '',
            labels: {},
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
            labels: subscriptionOptions.labels,
            filter: ''
          })
        })
      })
    })
  })

  describe('start', () => {
    const avroData = {
      first: 'one',
      second: 'two',
      third: undefined,
      createdAt: new Date('Thu Nov 05 2015 11:38:05 GMT-0800 (PST)'),
    }
    const avroDataPreserveNullMessageFromAvro = {
      first: 'one',
      second: 'two',
      third: undefined,
      createdAt: new Date('Thu Nov 05 2015 11:38:05 GMT-0800 (PST)'),
      now: { id: null, firstName: null },
    }

    it('receives avro parsed data', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([{}])
      topicMock.getMetadata.mockResolvedValue([{ schemaSettings: { schema: 'mock-schema' } }])
      schemaClientMock.getSchema.mockResolvedValue([{ definition: JSON.stringify(SCHEMA_DEFINITION_EXAMPLE) }])

      await subscriber.initialize()

      let parsedMessage: IParsedMessage<unknown> | undefined
      subscriber.start(msg => {
        parsedMessage = msg
        return Promise.resolve()
      })
      await subscriptionMock.receiveMessage(getMessageMock(Buffer.from(type.toString(avroData))))
      await flushPromises()
      expect(parsedMessage?.dataParsed).toEqual(avroData)
    })

    it('receives avro parsed data with null preserve fields', async () => {
      topicMock.exists.mockResolvedValue([false])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([{}])
      topicMock.getMetadata.mockResolvedValue([{ schemaSettings: { schema: 'mock-schema' } }])
      schemaClientMock.getSchema.mockResolvedValue([
        { definition: JSON.stringify(SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE) },
      ])

      await subscriber.initialize()

      const messageMock = getMessageMock(
        Buffer.from(typeWithPreserveNull.toString(avroDataPreserveNullMessageFromAvro)),
      )
      messageMock.attributes = { googclient_schemarevisionid: 'example', join_preserve_null: 'now' }

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
      subscriptionMock.getMetadata.mockResolvedValue([{}])
      topicMock.getMetadata.mockResolvedValue([{ schemaSettings: { schema: 'mock-schema' } }])
      schemaClientMock.listSchemaRevisions.mockResolvedValue([
        [{ revisionId: 'revision', definition: JSON.stringify(SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE) }],
      ])

      await subscriber.initialize()

      const messageMock = getMessageMock(Buffer.from(type.toString(avroData)))
      messageMock.attributes = {}

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
      subscriptionMock.getMetadata.mockResolvedValue([{}])
      topicMock.getMetadata.mockResolvedValue([{ schemaSettings: { schema: 'mock-schema' } }])

      schemaClientMock.getSchema.mockRejectedValue(new Error('NOT_FOUND'))
      schemaClientMock.listSchemaRevisions.mockResolvedValue([
        [{ revisionId: 'revision', definition: JSON.stringify(SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE) }],
      ])
      await subscriber.initialize()

      const messageMock = getMessageMock(Buffer.from(type.toString(avroData)))

      let parsedMessage: IParsedMessage<unknown> | undefined
      subscriber.start(msg => {
        parsedMessage = msg
        return Promise.resolve()
      })

      await subscriptionMock.receiveMessage(messageMock)
      await flushPromises()
      expect(parsedMessage?.dataParsed).toEqual(avroData)
    })

    it('receives avro parsed data and replaces empty array with undefined using path from metadata', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([{}])
      topicMock.getMetadata.mockResolvedValue([{ schemaSettings: { schema: 'mock-schema' } }])
      schemaClientMock.getSchema.mockResolvedValue([
        { definition: JSON.stringify(SCHEMA_DEFINITION_READER_OPTIONAL_ARRAY_EXAMPLE) },
      ])

      await subscriber.initialize()

      const publishedMessage = { first: 'one', tags: ['tag'] }
      const receivedMessage = { first: 'one', tags: ['tag'], languages: [] }
      const messageMock = getMessageMock(Buffer.from(readerTypeWithArrays.toString(receivedMessage)))
      messageMock.attributes = {
        googclient_schemarevisionid: 'example',
        join_undefined_or_null_optional_arrays: 'languages',
      }
      let parsedMessage: IParsedMessage<unknown> | undefined
      subscriber.start(msg => {
        parsedMessage = msg
        return Promise.resolve()
      })

      await subscriptionMock.receiveMessage(messageMock)
      await flushPromises()
      expect(parsedMessage?.dataParsed).toEqual(publishedMessage)
    })

    it('receives avro parsed data and replaces 2 empty array with undefined using path from metadata', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])
      subscriptionMock.getMetadata.mockResolvedValue([{}])
      topicMock.getMetadata.mockResolvedValue([{ schemaSettings: { schema: 'mock-schema' } }])
      schemaClientMock.getSchema.mockResolvedValue([
        { definition: JSON.stringify(SCHEMA_DEFINITION_READER_OPTIONAL_ARRAY_EXAMPLE) },
      ])

      await subscriber.initialize()

      const publishedMessage = { first: 'one' }
      const receivedMessage = { first: 'one', tags: [], languages: [] }
      const messageMock = getMessageMock(Buffer.from(readerTypeWithArrays.toString(receivedMessage)))
      messageMock.attributes = {
        googclient_schemarevisionid: 'example',
        join_undefined_or_null_optional_arrays: 'languages,tags',
      }
      let parsedMessage: IParsedMessage<unknown> | undefined
      subscriber.start(msg => {
        parsedMessage = msg
        return Promise.resolve()
      })

      await subscriptionMock.receiveMessage(messageMock)
      await flushPromises()
      expect(parsedMessage?.dataParsed).toEqual(publishedMessage)
    })

    it('unacknowledges message if processing fails', async () => {
      schemaClientMock.getSchema.mockResolvedValue([{ definition: JSON.stringify(SCHEMA_DEFINITION_EXAMPLE) }])
      subscriber.start(() => Promise.reject('Something wrong'))
      const messageMock = getMessageMock(Buffer.from(type.toString(avroData)))

      await subscriptionMock.receiveMessage(messageMock)
      await flushPromises()

      expect(messageMock.nack).toHaveBeenCalled()
    })

    it('unacknowledges message if message parsing fails', async () => {
      schemaClientMock.getSchema.mockResolvedValue([{ definition: JSON.stringify(SCHEMA_DEFINITION_EXAMPLE) }])
      subscriber.start(() => Promise.resolve())

      const messageMock = getMessageMock(Buffer.from('invalid-message'))
      await subscriptionMock.receiveMessage(messageMock)
      await flushPromises()

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
