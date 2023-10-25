import { ILogger } from './ILogger'
import { PublisherFactory } from './PublisherFactory'

export class PublisherInitializer<TypeMap> {
  private readonly publisherFactory: PublisherFactory<TypeMap>

  constructor(logger: ILogger, private readonly avroSchemas: Record<keyof TypeMap, { writer: object, reader: object }>) {
    this.publisherFactory = new PublisherFactory<TypeMap>(logger, avroSchemas)
  }

  public initializeAllPublishers(): Promise<void>[] {
    let topic: keyof TypeMap
    const initPromises: Promise<void>[] = []
    for (topic in this.avroSchemas) {
      const publisher = this.publisherFactory.getPublisher(topic)
      initPromises.push(publisher.initialize())
    }
    return initPromises
  }

}
