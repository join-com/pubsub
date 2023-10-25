import { Publisher } from '../Publisher'
import { PublisherInitializer } from '../PublisherInitializer'
import { ConsoleLogger, SCHEMA_DEFINITION_EXAMPLE } from './support/pubsubMock'

const publisherSpy = jest.spyOn(Publisher.prototype, 'initialize')

describe('PublisherInitializer.initializePublisher', () => {
  it('returns number of promises equal to the number of events in map', () => {
    const eventSchemas = {
      event1: { writer: SCHEMA_DEFINITION_EXAMPLE, reader: SCHEMA_DEFINITION_EXAMPLE },
      event2: { writer: SCHEMA_DEFINITION_EXAMPLE, reader: SCHEMA_DEFINITION_EXAMPLE }
    }
    publisherSpy.mockResolvedValue()
    const publisherInitializer = new PublisherInitializer<typeof eventSchemas>(new ConsoleLogger(), eventSchemas )
    const initPublisherPromises = publisherInitializer.initializeAllPublishers()

    expect(initPublisherPromises).toHaveLength(2)
  })

  it('returns empty array when there are no events', () => {
    const eventSchemas = {
    }
    publisherSpy.mockResolvedValue()
    const publisherInitializer = new PublisherInitializer<typeof eventSchemas>(new ConsoleLogger(), eventSchemas )
    const initPublisherPromises = publisherInitializer.initializeAllPublishers()

    expect(initPublisherPromises).toHaveLength(0)
  })
})
