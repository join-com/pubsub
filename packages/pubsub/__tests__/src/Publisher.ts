import { Publisher } from '../../src/Publisher'
import * as traceMock from '../../__mocks__/@join-com/node-trace'
import { getClientMock, getTopicMock } from '../support/pubsubMock'

const topic = 'topic-name'
const topicMock = getTopicMock()
const clientMock = getClientMock({ topicMock })

describe('Publisher', () => {
  let publisher: Publisher

  beforeEach(() => {
    publisher = new Publisher(topic, clientMock as any)
  })

  afterEach(() => {
    clientMock.topic.mockClear()

    topicMock.exists.mockReset()
    topicMock.create.mockReset()
    topicMock.publishJSON.mockReset()
    traceMock.getTraceContext.mockReset()
    traceMock.getTraceContextName.mockReset()
  })

  describe('initialize', () => {
    it('creates unless topic exists', async () => {
      topicMock.exists.mockResolvedValue([false])

      await publisher.initialize()

      expect(topicMock.create).toHaveBeenCalled()
      expect(clientMock.topic).toHaveBeenCalledWith(topic)
    })

    it('does not create if topic exists', async () => {
      topicMock.exists.mockResolvedValue([true])

      await publisher.initialize()

      expect(topicMock.create).not.toHaveBeenCalled()
      expect(clientMock.topic).toHaveBeenCalledWith(topic)
    })
  })

  describe('publishMsg', () => {
    const traceContext = 'trace-context'
    const traceContextName = 'trace-context-name'
    const message = { id: 1 }

    beforeEach(() => {
      traceMock.getTraceContext.mockReturnValue(traceContext)
      traceMock.getTraceContextName.mockReturnValue(traceContextName)
    })

    it('publishes json object with trace info', async () => {
      await publisher.publishMsg(message)

      expect(topicMock.publishJSON).toHaveBeenCalledWith(message, {
        [traceContextName]: traceContext
      })
    })

    it('publishes json array', async () => {
      const array = [message, message]
      await publisher.publishMsg(array)

      expect(topicMock.publishJSON).toHaveBeenCalledWith(array, {
        [traceContextName]: traceContext
      })
    })

    it('does not send trace info unless defined', async () => {
      traceMock.getTraceContext.mockReturnValue(undefined)

      await publisher.publishMsg({ id: 1 })

      expect(topicMock.publishJSON).toHaveBeenCalledWith(message, {})
    })
  })
})
