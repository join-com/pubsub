import * as traceMock from '../../__mocks__/@join-com/node-trace'
import { IParsedMessage, Subscriber } from '../../src/Subscriber'
import {
  getClientMock,
  getMessageMock,
  getSubscriptionMock,
  getTopicMock,
  MessageMock,
} from '../support/pubsubMock'

const topic = 'topic-name'
const subscription = 'subscription-name'
const subscriptionMock = getSubscriptionMock()
const topicMock = getTopicMock({ subscriptionMock })
const clientMock = getClientMock({ topicMock })
const baseOptions = {
  ackDeadline: 10,
  flowControl: {
    allowExcessMessages: false,
    maxMessages: 20,
  },
  streamingOptions: {
    highWaterMark: 30,
    maxStreams: 40,
    timeout: 50,
  },
}
const initializationOptions = {
  deadLetterPolicy: {
    maxDeliveryAttempts: 11,
  },
}
const options = {
  ...baseOptions,
  ...initializationOptions,
}

describe('Subscriber', () => {
  let subscriber: Subscriber

  beforeEach(() => {
    subscriber = new Subscriber(topic, subscription, clientMock as any, options)
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
  })

  describe('initialize', () => {
    it('creates topic unless exists', async () => {
      topicMock.exists.mockResolvedValue([false])
      subscriptionMock.exists.mockResolvedValue([true])

      await subscriber.initialize()

      expect(topicMock.create).toHaveBeenCalled()
      expect(clientMock.topic).toHaveBeenCalledWith(topic)
    })

    it('does not create topic if exists', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])

      await subscriber.initialize()

      expect(topicMock.create).not.toHaveBeenCalled()
      expect(clientMock.topic).toHaveBeenCalledWith(topic)
    })

    it('creates subscription unless exists', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([false])

      await subscriber.initialize()

      expect(subscriptionMock.create).toHaveBeenCalledWith({
        deadLetterPolicy: {
          ...initializationOptions.deadLetterPolicy,
          deadLetterTopic: subscription + '-dead-letters',
        },
      })
      expect(topicMock.subscription).toHaveBeenCalledWith(
        subscription,
        baseOptions
      )
    })

    it('does not create subscription if exists', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([true])

      await subscriber.initialize()

      expect(subscriptionMock.create).not.toHaveBeenCalled()
      expect(topicMock.subscription).toHaveBeenCalledWith(
        subscription,
        baseOptions
      )
    })

    it('does not add dead letter policy when not requested', async () => {
      topicMock.exists.mockResolvedValue([true])
      subscriptionMock.exists.mockResolvedValue([false])

      const emptyOptions = {}
      const optionlessSubscriber = new Subscriber(
        topic,
        subscription,
        clientMock as any,
        emptyOptions
      )

      await optionlessSubscriber.initialize()

      expect(subscriptionMock.create).toHaveBeenCalledWith({})
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
      subscriber.start(async (msg) => {
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
