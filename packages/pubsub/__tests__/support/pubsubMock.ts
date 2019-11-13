type EventHandler = (attrs: unknown) => Promise<unknown>
type EventHandlerMap = { [key: string]: EventHandler }

export const getSubscriptionMock = () => {
  const eventHandlers: EventHandlerMap = {}
  return {
    exists: jest.fn(),
    create: jest.fn(),
    on: (event: string, handler: EventHandler) => {
      eventHandlers[event] = handler
    },
    receiveMessage: async (message: MessageMock) => {
      const handler = eventHandlers['message']
      if (handler) {
        await handler(message)
      }
    }
  }
}

export interface TopicMockOption {
  subscriptionMock?: any
}

export const getTopicMock = ({ subscriptionMock }: TopicMockOption = {}) => ({
  exists: jest.fn(),
  create: jest.fn(),
  publishJSON: jest.fn(),
  subscription: jest.fn(() => subscriptionMock)
})

export interface ClientMockOption {
  topicMock?: any
}

export const getClientMock = ({ topicMock }: ClientMockOption = {}) => ({
  topic: jest.fn(() => topicMock)
})

export interface MessageMock {
  data: Buffer
  attributes: {}
  ack: jest.Mock<any, any>
  nack: jest.Mock<any, any>
}

export const getMessageMock = (data: any, attributes: {} = {}): MessageMock => {
  const buffer = Buffer.from(JSON.stringify(data), 'utf8')
  return {
    data: buffer,
    attributes,
    ack: jest.fn(),
    nack: jest.fn()
  }
}
