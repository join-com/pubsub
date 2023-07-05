import { PubSub } from '@google-cloud/pubsub';
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1';
import { SCHEMA_NAME_SUFFIX, SchemaDeployer } from '../SchemaDeployer'

const processApplicationStateStringSchema = '{"type":"record","name":"ProcessApplicationState","fields":[{"name":"applicationId","type":["null","int"],"default":null}],"Event":"data-cmd-process-application-state","SchemaType":"READER","AvdlSchemaVersion":"4adb1df1c9243e24b937ddd165abf7572d7e2491","AvdlSchemaGitRemoteOriginUrl":"git@github.com:join-com/data.git","AvdlSchemaPathInGitRepo":"schemas/avro/commands/commands.avdl","GeneratorVersion":"387a0b3f2c890dc67f99085b7c94ff4bdc9cc967","GeneratorGitRemoteOriginUrl":"https://github.com/join-com/avro-join"}'
const processApplicationStateReaderSchema = {'reader':{'type':'record','name':'ProcessApplicationState','fields':[{'name':'applicationId','type':['null','int'],'default':null}],'Event':'data-cmd-process-application-state','SchemaType':'READER','AvdlSchemaVersion':'4adb1df1c9243e24b937ddd165abf7572d7e2491','AvdlSchemaGitRemoteOriginUrl':'git@github.com:join-com/data.git','AvdlSchemaPathInGitRepo':'schemas/avro/commands/commands.avdl','GeneratorVersion':'387a0b3f2c890dc67f99085b7c94ff4bdc9cc967','GeneratorGitRemoteOriginUrl':'https://github.com/join-com/avro-join'}}
const processApplicationStateReaderSchemaUpdated = {'reader':{'type':'record','name':'ProcessApplicationState','fields':[{'name':'applicationId','type':['null','int'],'default':null},{'name':'userId','type':['null','int'],'default':null}],'Event':'data-cmd-process-application-state','SchemaType':'READER','AvdlSchemaVersion':'4adb1df1c9243e24b937ddd165abf7572d7e2491','AvdlSchemaGitRemoteOriginUrl':'git@github.com:join-com/data.git','AvdlSchemaPathInGitRepo':'schemas/avro/commands/commands.avdl','GeneratorVersion':'387a0b3f2c890dc67f99085b7c94ff4bdc9cc967','GeneratorGitRemoteOriginUrl':'https://github.com/join-com/avro-join'}}

const processApplicationStateGCloudSchema = {
    type: 'AVRO',
    name: 'data-company-affiliate-referral-created',
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
    commitSchema: jest.fn()
})

describe('deployAvroSchemas', () => {
    it('does nothing and logs when no enabled avro topics', async () => {
        const schemasToDeploy = {
            'data-user-created': false,
            'data-user-deleted': false,
        }
        const logger = getLoggerMock();
        const schemaDeployer = new SchemaDeployer(logger, getPubsubMock() as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)

        await schemaDeployer.deployAvroSchemas(schemasToDeploy, {})

        expect(logger.info).toHaveBeenCalledWith('Finished deployAvroSchemas, no schemas to deploy')
    })

    it('does nothing and logs when schema exist', async () => {
        const logger = getLoggerMock();
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield processApplicationStateGCloudSchema
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable);
        const schemaDeployer = new SchemaDeployer(logger, pubsubMock as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)

        const schemasToDeploy = { 'data-cmd-process-application-state': true }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchema};

        await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(logger.info).toHaveBeenLastCalledWith('Finished deployAvroSchemas, all schemas are already deployed')
    })

    it('creates schema when it doesn\'t exist', async () => {
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            async *[Symbol.asyncIterator]() {
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable);
        const schemaDeployer = new SchemaDeployer(getLoggerMock(), pubsubMock as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)
        const schemasToDeploy = {
            'data-cmd-process-application-state': true,
        }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchema};

        await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(pubsubMock.createSchema).toHaveBeenCalledWith('data-cmd-process-application-state-generated-avro', 'AVRO', JSON.stringify(readerSchemas['data-cmd-process-application-state'].reader))
    })

    it('creates schema revision when schemas don\'t match', async () => {
        const projectName = 'project'
        process.env['GCLOUD_PROJECT'] = projectName
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
            'data-cmd-process-application-state': true,
        }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchemaUpdated};

        await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        const schemaName = 'data-cmd-process-application-state' + SCHEMA_NAME_SUFFIX
        const schemaPath = `projects/${projectName}/schemas/${schemaName}`
        expect(schemaServiceClientMock.commitSchema).toHaveBeenCalledWith({
            name: schemaPath, schema: {
                name: schemaPath, type: 'AVRO', definition: JSON.stringify(processApplicationStateReaderSchemaUpdated.reader),
            },
        })
    })
})
