/* eslint-disable @typescript-eslint/no-explicit-any */
type EventHandler = (attrs: unknown) => Promise<unknown>
type EventHandlerMap = { [key: string]: EventHandler }

export const SCHEMA_DEFINITION_EXAMPLE = {
  'type': 'record',
  'name': 'Avro',
  'fields': [
    {
      'name': 'first',
      'type': 'string'
    },
    {
      'name': 'second',
      'type': 'string',
      'default': ''
    },
    {
      'name': 'third',
      'type': [
        'null',
        'string'
      ],
      'default': null
    }
  ]
}
const SCHEMA_EXAMPLE = {definition: JSON.stringify(SCHEMA_DEFINITION_EXAMPLE)}

export const getIamMock = () => ({
  setPolicy: jest.fn(),
})

export interface ISubscriptionMockOption {
  iamMock?: ReturnType<typeof getIamMock>
}

export const getSubscriptionMock = ({ iamMock }: ISubscriptionMockOption = {}) => {
  const eventHandlers: EventHandlerMap = {}
  return {
    exists: jest.fn(),
    create: jest.fn(),
    setMetadata: jest.fn(),
    close: jest.fn(),
    open: jest.fn(),
    iam: iamMock,
    on: (event: string, handler: EventHandler) => {
      eventHandlers[event] = handler
    },
    receiveMessage: async (message: IMessageMock) => {
      const handler = eventHandlers['message']
      if (handler) {
        await handler(message)
      }
    },
    emitError: async (err: Error) => {
      const handler = eventHandlers['error']
      if (handler) {
        await handler(err)
      }
    },
    emitClose: async () => {
      const handler = eventHandlers['close']
      if (handler) {
        // undefined means no error when closing subscriber
        await handler(undefined)
      }
    },
  }
}

export interface ITopicMockOption {
  subscriptionMock?: ReturnType<typeof getSubscriptionMock>
  iamMock?: ReturnType<typeof getIamMock>
}

export const getTopicMock = ({ subscriptionMock, iamMock }: ITopicMockOption = {}) => ({
  exists: jest.fn(),
  create: jest.fn(),
  flush: jest.fn(),
  publishMessage: jest.fn(),
  subscription: jest.fn(() => subscriptionMock),
  iam: iamMock,
  getMetadata: jest.fn()
})

export const schemaMock = {
  get: jest.fn(() => Promise.resolve(SCHEMA_EXAMPLE)),
}

export interface IClientMockOption {
  topicMock?: ReturnType<typeof getTopicMock>
}

export const getClientMock = ({ topicMock }: IClientMockOption = {}) => ({
  topic: jest.fn(() => topicMock),
  schema: jest.fn(() => schemaMock)
})

export interface IMessageMock {
  data: Buffer
  ack: jest.Mock<unknown, any>
  nack: jest.Mock<unknown, any>
}

export const getMessageMock = (data: unknown): IMessageMock => {
  const buffer = Buffer.from(JSON.stringify(data), 'utf8')
  return {
    data: buffer,
    ack: jest.fn(),
    nack: jest.fn(),
  }
}
