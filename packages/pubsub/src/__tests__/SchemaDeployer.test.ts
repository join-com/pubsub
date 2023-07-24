import { PubSub } from '@google-cloud/pubsub';
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1';
import { SCHEMA_NAME_SUFFIX, SchemaDeployer } from '../SchemaDeployer'

const processApplicationStateStringSchema = '{"type":"record","name":"ProcessApplicationState","fields":[{"name":"applicationId","type":["null","int"],"default":null}],"Event":"data-cmd-process-application-state","SchemaType":"READER","AvdlSchemaVersion":"4adb1df1c9243e24b937ddd165abf7572d7e2491","AvdlSchemaGitRemoteOriginUrl":"git@github.com:join-com/data.git","AvdlSchemaPathInGitRepo":"schemas/avro/commands/commands.avdl","GeneratorVersion":"387a0b3f2c890dc67f99085b7c94ff4bdc9cc967","GeneratorGitRemoteOriginUrl":"https://github.com/join-com/avro-join"}'
const processApplicationStateReaderSchema = {'reader':{'type':'record','name':'ProcessApplicationState','fields':[{'name':'applicationId','type':['null','int'],'default':null}],'Event':'data-cmd-process-application-state','SchemaType':'READER','AvdlSchemaVersion':'4adb1df1c9243e24b937ddd165abf7572d7e2491','AvdlSchemaGitRemoteOriginUrl':'git@github.com:join-com/data.git','AvdlSchemaPathInGitRepo':'schemas/avro/commands/commands.avdl','GeneratorVersion':'387a0b3f2c890dc67f99085b7c94ff4bdc9cc967','GeneratorGitRemoteOriginUrl':'https://github.com/join-com/avro-join'}}
const processApplicationStateReaderSchemaUpdated = {'reader':{'type':'record','name':'ProcessApplicationState','fields':[{'name':'applicationId','type':['null','int'],'default':null},{'name':'userId','type':['null','int'],'default':null}],'Event':'data-cmd-process-application-state','SchemaType':'READER','AvdlSchemaVersion':'4adb1df1c9243e24b937ddd165abf7572d7e2491','AvdlSchemaGitRemoteOriginUrl':'git@github.com:join-com/data.git','AvdlSchemaPathInGitRepo':'schemas/avro/commands/commands.avdl','GeneratorVersion':'387a0b3f2c890dc67f99085b7c94ff4bdc9cc967','GeneratorGitRemoteOriginUrl':'https://github.com/join-com/avro-join'}}

const processApplicationStateGCloudSchema = {
    type: 'AVRO',
    name: 'data-company-affiliate-referral-created' + SCHEMA_NAME_SUFFIX,
    definition: processApplicationStateStringSchema
}

const getLoggerMock = () => ({
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
})

type ListSchemaAsyncIteratorMock = { [Symbol.asyncIterator](): AsyncIterableIterator<{ name: string; definition: string }> }
const getPubsubMock = (asyncIterable: ListSchemaAsyncIteratorMock
                         = undefined as unknown as ListSchemaAsyncIteratorMock) => ({
    listSchemas: jest.fn().mockReturnValue(asyncIterable),
    createSchema: jest.fn()
})
const getSchemaServiceClientMock = () => ({
    getSchema: jest.fn(),
    commitSchema: jest.fn(),
    deleteSchema: jest.fn()
})

const emptyAsyncIterable = {
    // eslint-disable-next-line @typescript-eslint/no-empty-function
    async *[Symbol.asyncIterator]() {
    }
}

describe('deployAvroSchemas', () => {
    beforeEach(() => {
        process.env['GCLOUD_PROJECT'] = 'project'
    })

    it('does nothing when no enabled topics, and no existing schemas', async () => {
        const schemasToDeploy = {
            'data-user-created': false,
            'data-user-deleted': false,
        }
        const schemaDeployer = new SchemaDeployer(getLoggerMock(), getPubsubMock(emptyAsyncIterable) as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)

        const { schemasCreated, revisionsCreated, schemasDeleted} = await schemaDeployer.deployAvroSchemas(schemasToDeploy, {})

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(0)
        expect(schemasDeleted).toBe(0)
    })

    it('throws error when non-avro schema exists with avro name', async () => {
        const nonAvroSchemaWithAvroName = {
            type: 'PROTOCOL_BUFFER',
            name: 'data-company-affiliate-referral-created' + SCHEMA_NAME_SUFFIX,
            definition: 'some'
        }
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield nonAvroSchemaWithAvroName
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable);
        const schemaDeployer = new SchemaDeployer(getLoggerMock(), pubsubMock as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)

        const schemasToDeploy = { 'data-company-affiliate-referral-created': true }
        const readerSchemas = {'data-company-affiliate-referral-created': processApplicationStateReaderSchema};

        await expect(schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)).rejects
          .toThrow('Non avro schema exists for avro topic \'data-company-affiliate-referral-created-generated-avro\', please remove it before starting the service')
    })

    it('does nothing and logs when schema revisions match', async () => {
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield processApplicationStateGCloudSchema
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable);
        const schemaDeployer = new SchemaDeployer(getLoggerMock(), pubsubMock as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)

        const schemasToDeploy = { 'data-company-affiliate-referral-created': true }
        const readerSchemas = {'data-company-affiliate-referral-created': processApplicationStateReaderSchema};

        const { schemasCreated, revisionsCreated, schemasDeleted } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(0)
        expect(schemasDeleted).toBe(0)
    })

    it('creates schema when it doesn\'t exist', async () => {
        const pubsubMock = getPubsubMock(emptyAsyncIterable);
        const schemaDeployer = new SchemaDeployer(getLoggerMock(), pubsubMock as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)
        const schemasToDeploy = {
            'data-cmd-process-application-state': true,
        }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchema};

        const { schemasCreated, revisionsCreated } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(1)
        expect(revisionsCreated).toBe(0)
        expect(pubsubMock.createSchema).toHaveBeenCalledWith('data-cmd-process-application-state-generated-avro', 'AVRO', JSON.stringify(readerSchemas['data-cmd-process-application-state'].reader))
    })

    it('creates schema revision when schemas don\'t match', async () => {
        const processApplicationStateGCloudSchema = {
            type: 'AVRO',
            name: 'data-company-affiliate-referral-created-generated-avro',
            definition: processApplicationStateStringSchema
        }
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield processApplicationStateGCloudSchema
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable);
        const schemaServiceClientMock = getSchemaServiceClientMock() as unknown as SchemaServiceClient
        const schemaDeployer = new SchemaDeployer(getLoggerMock(), pubsubMock as unknown as PubSub,
          schemaServiceClientMock)
        const schemasToDeploy = {
            'data-company-affiliate-referral-created': true,
        }
        const readerSchemas = {'data-company-affiliate-referral-created': processApplicationStateReaderSchemaUpdated};

        const { schemasCreated, revisionsCreated } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(1)
        const schemaName = 'data-company-affiliate-referral-created' + SCHEMA_NAME_SUFFIX
        const schemaPath = `projects/${process.env['GCLOUD_PROJECT'] as string}/schemas/${schemaName}`
        expect(schemaServiceClientMock.commitSchema).toHaveBeenCalledWith({
            name: schemaPath, schema: {
                name: schemaPath, type: 'AVRO', definition: JSON.stringify(processApplicationStateReaderSchemaUpdated.reader),
            },
        })
    })

    it('delete existing schema when it is disabled', async () => {
        const processApplicationStateGCloudSchema = {
            type: 'AVRO',
            name: 'data-company-affiliate-referral-created-generated-avro',
            definition: processApplicationStateStringSchema
        }
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield processApplicationStateGCloudSchema
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable);
        const schemaServiceClientMock = getSchemaServiceClientMock() as unknown as SchemaServiceClient
        const schemaDeployer = new SchemaDeployer(getLoggerMock(), pubsubMock as unknown as PubSub,
          schemaServiceClientMock)
        const schemasToDeploy = {
            'data-company-affiliate-referral-created': false,
        }
        const readerSchemas = {'data-company-affiliate-referral-created': processApplicationStateReaderSchemaUpdated};

        const { schemasCreated, revisionsCreated, schemasDeleted } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(0)
        expect(schemasDeleted).toBe(1)
        const schemaName = 'data-company-affiliate-referral-created' + SCHEMA_NAME_SUFFIX
        expect(schemaServiceClientMock.deleteSchema).toHaveBeenCalledWith({
            name: schemaName
        })
    })
})