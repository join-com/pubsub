import * as traceMock from '../../__mocks__/@join-com/node-trace'
import {
  IParsedMessage,
  ISubscriptionOptions,
  Subscriber
} from '../../src/Subscriber'
import {
  getClientMock,
  getIamMock,
  getMessageMock,
  getSubscriptionMock,
  getTopicMock,
  MessageMock
} from '../support/pubsubMock'

const topicName = 'topic-name'
const subscriptionName = 'subscription-name'

const iamSubscriptionMock = getIamMock()
const subscriptionMock = getSubscriptionMock({ iamMock: iamSubscriptionMock })
const iamTopicMock = getIamMock()
const topicMock = getTopicMock({ subscriptionMock, iamMock: iamTopicMock })
const clientMock = getClientMock({ topicMock })
const options: ISubscriptionOptions = {
  ackDeadline: 10,
  allowExcessMessages: true,
  maxMessages: 5,
  maxStreams: 1
}

describe('Subscriber', () => {
  let subscriber: Subscriber

  beforeEach(() => {
    subscriber = new Subscriber(
      topicName,
      subscriptionName,
      clientMock as any,
      options
    )
  })

  afterEach(() => {
    clientMock.topic.mockClear()
    topicMock.subscription.mockClear()

    topicMock.exists.mockReset()
    topicMock.create.mockReset()
    subscriptionMock.exists.mockReset()
    subscriptionMock.create.mockReset()
    traceMock.getTraceContextName.mockReset()
    traceMock.start.mockReset()
    iamTopicMock.setPolicy.mockReset()
    iamSubscriptionMock.setPolicy.mockReset()
  })

  describe('initialize', () => {
    it('creates topic unless exists', async () => {
      topicMock.exists.mockResolvedValue([false])
      subscriptionMock.exists.mockResolvedValue([true])

      await subscriber.initialize()

      expect(topicMock.create).toHaveBeenCalledTimes(1)
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

      expect(subscriptionMock.create).toHaveBeenCalled()
      expect(topicMock.subscription).toHaveBeenCalledWith(subscriptionName, {
        ackDeadline: options.ackDeadline,
        flowControl: {
          allowExcessMessages: options.allowExcessMessages,
          maxMessages: options.maxMessages
        },
        streamingOptions: {
          maxStreams: options.maxStreams
        }
      })
    })

    it('does not create subscription if exists', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
    })

    describe('dead letter policy', () => {
      const deadLetterTopicName = 'subscription-name-dead-letters'
      const deadLetterSubscriptionName =
        'subscription-name-dead-letters-subscription'

      const deadLetterOptions: ISubscriptionOptions = {
        ...options,
        maxDeliveryAttempts: 123,
        gcloudProject: {
          name: 'gcloudProjectName',
          id: 123456789
        }
      }

      beforeEach(() => {
        subscriber = new Subscriber(
          topicName,
          subscriptionName,
          clientMock as any,
          deadLetterOptions
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
                members: [
                  'serviceAccount:service-123456789@gcp-sa-pubsub.iam.gserviceaccount.com'
                ],
                role: 'roles/pubsub.publisher'
              }
            ]
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
            topicName,
            subscriptionName,
            clientMock as any,
            emptyOptions
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
          expect(subscriptionMock.create).toHaveBeenLastCalledWith(undefined)
          expect(topicMock.subscription).toHaveBeenLastCalledWith(
            deadLetterSubscriptionName
          )
        })

        it('adds subscriber role to pubsub service account', async () => {
          subscriptionMock.exists.mockResolvedValue([false])

          await subscriber.initialize()

          expect(iamSubscriptionMock.setPolicy).toHaveBeenCalledWith({
            bindings: [
              {
                members: [
                  'serviceAccount:service-123456789@gcp-sa-pubsub.iam.gserviceaccount.com'
                ],
                role: 'roles/pubsub.subscriber'
              }
            ]
          })
        })

        it('does not create deadLetterSubscription if exists', async () => {
          subscriptionMock.exists.mockResolvedValue([true])

          await subscriber.initialize()

          expect(subscriptionMock.create).not.toHaveBeenCalled()
          expect(topicMock.subscription).toHaveBeenLastCalledWith(
            deadLetterSubscriptionName
          )
        })

        it('does not create deadLetterSubscription if not necessary', async () => {
          subscriptionMock.exists.mockResolvedValue([false])
          const emptyOptions = {}
          const optionlessSubscriber = new Subscriber(
            topicName,
            subscriptionName,
            clientMock as any,
            emptyOptions
          )

          await optionlessSubscriber.initialize()

          expect(subscriptionMock.create).toHaveBeenCalledTimes(1)
          expect(subscriptionMock.create).toHaveBeenCalledWith(undefined)
          expect(topicMock.subscription).not.toHaveBeenCalledWith(
            deadLetterSubscriptionName,
            expect.anything()
          )
          expect(iamSubscriptionMock.setPolicy).not.toHaveBeenCalled()
        })

        it('creates subscription with deadLetterTopic reference', async () => {
          subscriptionMock.exists.mockResolvedValue([false])

          await subscriber.initialize()

          expect(subscriptionMock.create).toHaveBeenCalledWith({
            deadLetterPolicy: {
              maxDeliveryAttempts: 123,
              deadLetterTopic:
                'projects/gcloudProjectName/topics/subscription-name-dead-letters'
            }
          })
        })
      })
    })
  })

  describe('start', () => {
    const data = { id: 1, createdAt: new Date() }

    let messageMock: MessageMock

    beforeEach(() => {
      const traceContextName = 'trace-context-name'
      traceMock.getTraceContextName.mockReturnValue(traceContextName)

      const attributes = { [traceContextName]: 'trace-context' }
      messageMock = getMessageMock(data, attributes)
    })

    it('receives parsed data', async () => {
      let parsedMessage: IParsedMessage<unknown>
      subscriber.start(async msg => {
        parsedMessage = msg
      })

      await subscriptionMock.receiveMessage(messageMock)

      expect(parsedMessage.dataParsed).toEqual(data)
      expect(traceMock.start).toHaveBeenCalledWith('trace-context')
    })

    it('unacknowledges message if processing fails', async () => {
      subscriber.start(async () => {
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
