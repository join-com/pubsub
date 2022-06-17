import { PubSub } from '@google-cloud/pubsub'
import { createCallOptions } from '../../src/createCallOptions'
import { IParsedMessage, ISubscriptionOptions, Subscriber } from '../../src/Subscriber'
import {
  getClientMock,
  getIamMock,
  getMessageMock,
  getSubscriptionMock,
  getTopicMock,
  IMessageMock,
} from './support/pubsubMock'

const topicName = 'topic-name'
const subscriptionName = 'subscription-name'

const iamSubscriptionMock = getIamMock()
const subscriptionMock = getSubscriptionMock({ iamMock: iamSubscriptionMock })
const iamTopicMock = getIamMock()
const topicMock = getTopicMock({ subscriptionMock, iamMock: iamTopicMock })
const clientMock = getClientMock({ topicMock })
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
    subscriber = new Subscriber({ topicName, subscriptionName, subscriptionOptions }, clientMock as unknown as PubSub)
  })

  describe('initialize', () => {
    it('creates topic unless exists', async () => {
      topicMock.exists.mockResolvedValue([false])
      subscriptionMock.exists.mockResolvedValue([true])

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

      subscriber = new Subscriber({ topicName, subscriptionName }, clientMock as unknown as PubSub)

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
          clientMock as unknown as PubSub,
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
            clientMock as unknown as PubSub,
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
            clientMock as unknown as PubSub,
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

    it('unacknowledges message if processing fails', async () => {
      subscriber.start(() => {
        throw new Error('Something wrong')
      })

      await subscriptionMock.receiveMessage(messageMock)

      expect(messageMock.nack).toHaveBeenCalled()
    })

    it('restarts subscription on error', async () => {
      subscriber.start(Promise.resolve)

      await subscriptionMock.emitError(new Error('boom'))

      expect(subscriptionMock.close).toHaveBeenCalled()
      expect(subscriptionMock.open).toHaveBeenCalled()
    })
  })
})
