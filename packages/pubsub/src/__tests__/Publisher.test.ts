import { PubSub } from '@google-cloud/pubsub'
import { createCallOptions } from '../../src/createCallOptions'
import { Publisher } from '../../src/Publisher'
import { getClientMock, getTopicMock } from './support/pubsubMock'

const topic = 'topic-name'
const topicMock = getTopicMock()
const clientMock = getClientMock()

describe('Publisher', () => {
  let publisher: Publisher

  beforeEach(() => {
    publisher = new Publisher(topic, clientMock as unknown as PubSub)
  })

  describe('initialize', () => {
    it('creates unless topic exists', async () => {
      topicMock.exists.mockResolvedValue([false])

      await publisher.initialize()

      expect(topicMock.create).toHaveBeenCalledWith(createCallOptions)
      expect(clientMock.topic).toHaveBeenCalledWith(topic)
    })

    //     it('does not create if topic exists', async () => {
    //       topicMock.exists.mockResolvedValue([true])

    //       await publisher.initialize()

    //       expect(topicMock.create).not.toHaveBeenCalled()
    //       expect(clientMock.topic).toHaveBeenCalledWith(topic)
    //     })
  })

  //   describe('publishMsg', () => {
  //     const traceContext = 'trace-context'
  //     const traceContextName = 'trace-context-name'
  //     const message = { id: 1 }

  //     it('publishes json object with trace info', async () => {
  //       await publisher.publishMsg(message)

  //       expect(topicMock.publishJSON).toHaveBeenCalledWith(message, {
  //         [traceContextName]: traceContext,
  //       })
  //     })

  //     it('publishes json array', async () => {
  //       const array = [message, message]
  //       await publisher.publishMsg(array)

  //       expect(topicMock.publishJSON).toHaveBeenCalledWith(array, {
  //         [traceContextName]: traceContext,
  //       })
  //     })
  //   })
})
