/* eslint-disable @typescript-eslint/no-explicit-any */
import { ILogger } from '../../ILogger'

type EventHandler = (attrs: unknown) => Promise<unknown>
type EventHandlerMap = { [key: string]: EventHandler }

export interface IMessageType {
  first?: string,
  second?: string,
  createdAt?: Date,
  third?: string,
  fourth?: {flag: boolean}
}

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
  get: jest.fn(),
}

export const schemaServiceClientMock = {
  getSchema: jest.fn(),
  listSchemaRevisions: jest.fn()
}

export const subscriberServiceClientMock = {
  getSubscription: jest.fn()
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
  attributes?: Record<string, string>
}

export const getMessageMock = (data: unknown): IMessageMock => {
  const buffer = Buffer.from(JSON.stringify(data), 'utf8')
  return {
    data: buffer,
    ack: jest.fn(),
    nack: jest.fn(),
    attributes: { }
  }
}

export class ConsoleLogger implements ILogger {
  error(message: string, payload: unknown | undefined): void {
    // eslint-disable-next-line no-console
    console.log(message, payload)
  }

  info(message: string, payload: unknown | undefined): void {
    // eslint-disable-next-line no-console
    console.log(message, payload)
  }

  warn(message: string, payload: unknown | undefined): void {
    // eslint-disable-next-line no-console
    console.log(message, payload)
  }
}
