import { SchemaServiceClient, SubscriberClient } from '@google-cloud/pubsub/build/src/v1'
import { SchemaCache } from '../SchemaCache'
import { SCHEMA_DEFINITION_EXAMPLE } from './support/constants'
import { schemaServiceClientMock, subscriberServiceClientMock } from './support/pubsubMock'

const schemaClientMock = schemaServiceClientMock
const subscriberClientMock = subscriberServiceClientMock


describe('SchemaCache', () => {

  describe('getTypeFromCacheOrRemote', () => {
    it('gets schema from real topic for unack topic', async () => {
      process.env['GCLOUD_PROJECT'] = 'project'
      schemaClientMock.getSchema.mockResolvedValueOnce([{definition: JSON.stringify(SCHEMA_DEFINITION_EXAMPLE)}])
      subscriberClientMock.getSubscription.mockResolvedValueOnce([{topic: 'real-topic'}])

      const schemaCache = new SchemaCache(schemaClientMock as unknown as SchemaServiceClient,
        subscriberClientMock as unknown as SubscriberClient, 'topic-sub-unack')

      await schemaCache.getTypeFromCacheOrRemote('revisionId')

      expect(schemaClientMock.getSchema)
        .toHaveBeenCalledWith({ name: 'projects/project/schemas/real-topic-generated-avro@revisionId'} )
      expect(subscriberClientMock.getSubscription).toHaveBeenCalled()
    })
  })
})
