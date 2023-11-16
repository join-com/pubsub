import { ILogger } from '../ILogger'
import { Subscriber } from '../Subscriber'
import { SubscriberFactory } from '../SubscriberFactory'

jest.mock('../Subscriber')

const SubscriberMock = jest.mocked(Subscriber)

interface IEvents {
  topic: { id: number }
}

const loggerMock = jest.mocked<ILogger>({
  info: jest.fn(),
  warn: jest.fn(),
  error: jest.fn(),
})

describe('SubscriberFactory', () => {
  let subscriberFactory: SubscriberFactory<IEvents>

  beforeEach(() => {
    subscriberFactory = new SubscriberFactory({ maxStreams: 20 }, loggerMock)
  })

  afterEach(() => {
    SubscriberMock.mockClear()
  })

  it('creates subscribers with the same client if the limit of 80 connections is not reached', () => {
    subscriberFactory.getSubscriber('topic', 'subscription-one')
    subscriberFactory.getSubscriber('topic', 'subscription-one')

    // Verify that Subscriber constructors were called with the same PubSub client
    expect(SubscriberMock.mock.calls[0]?.[1]).toEqual(SubscriberMock.mock.calls[1]?.[1])
  })

  it('creates subscribers with the different clients if the limit of 80 connections is reached', () => {
    subscriberFactory.getSubscriber('topic', 'subscription-one')
    subscriberFactory.getSubscriber('topic', 'subscription-two')
    subscriberFactory.getSubscriber('topic', 'subscription-three')
    subscriberFactory.getSubscriber('topic', 'subscription-four')
    subscriberFactory.getSubscriber('topic', 'subscription-five')

    // Verify that Subscriber constructors were called with different PubSub clients
    expect(SubscriberMock.mock.calls[0]?.[1]).not.toEqual(SubscriberMock.mock.calls[4]?.[1])
    expect(loggerMock.info).toHaveBeenCalledWith(
      'SubscriberFactory: Reached PubSub client connections limit. Creating new client.',
    )
  })
})
