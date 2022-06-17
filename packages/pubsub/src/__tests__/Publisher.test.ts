import { PubSub } from '@google-cloud/pubsub'
import { createCallOptions } from '../../src/createCallOptions'
import { Publisher } from '../../src/Publisher'
import { getClientMock, getTopicMock } from './support/pubsubMock'

const topic = 'topic-name'
const topicMock = getTopicMock()
const clientMock = getClientMock({ topicMock })

describe('Publisher', () => {
  let publisher: Publisher

  beforeEach(() => {
    publisher = new Publisher(topic, clientMock as unknown as PubSub)
  })

  afterEach(() => {
    clientMock.topic.mockClear()
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
  })

  describe('publishMsg', () => {
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
})
