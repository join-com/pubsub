import { PubSub } from '@google-cloud/pubsub'
import { SchemaServiceClient } from '@google-cloud/pubsub/build/src/v1'
import { MAX_REVISIONS_IN_GCLOUD, SCHEMA_NAME_SUFFIX, SchemaDeployer } from '../SchemaDeployer'
import { loggerMock } from './support/pubsubMock'

const processApplicationStateStringSchema = '{"type":"record","name":"ProcessApplicationState","fields":[{"name":"applicationId","type":["null","int"],"default":null}],"Event":"data-cmd-process-application-state","SchemaType":"READER","AvdlSchemaVersion":"4adb1df1c9243e24b937ddd165abf7572d7e2491","AvdlSchemaGitRemoteOriginUrl":"git@github.com:join-com/data.git","AvdlSchemaPathInGitRepo":"schemas/avro/commands/commands.avdl","GeneratorVersion":"387a0b3f2c890dc67f99085b7c94ff4bdc9cc967","GeneratorGitRemoteOriginUrl":"https://github.com/join-com/avro-join"}'
const processApplicationStateReaderSchema = {'reader':{'type':'record','name':'ProcessApplicationState','fields':[{'name':'applicationId','type':['null','int'],'default':null}],'Event':'data-cmd-process-application-state','SchemaType':'READER','AvdlSchemaVersion':'4adb1df1c9243e24b937ddd165abf7572d7e2491','AvdlSchemaGitRemoteOriginUrl':'git@github.com:join-com/data.git','AvdlSchemaPathInGitRepo':'schemas/avro/commands/commands.avdl','GeneratorVersion':'387a0b3f2c890dc67f99085b7c94ff4bdc9cc967','GeneratorGitRemoteOriginUrl':'https://github.com/join-com/avro-join'}}
const processApplicationStateReaderSchemaOnlyCommitChanged = {'reader':{'type':'record','name':'ProcessApplicationState','fields':[{'name':'applicationId','type':['null','int'],'default':null}],'Event':'data-cmd-process-application-state','SchemaType':'READER','AvdlSchemaVersion':'differentCommitHash','AvdlSchemaGitRemoteOriginUrl':'git@github.com:join-com/data.git','AvdlSchemaPathInGitRepo':'schemas/avro/commands/commands.avdl','GeneratorVersion':'387a0b3f2c890dc67f99085b7c94ff4bdc9cc967','GeneratorGitRemoteOriginUrl':'https://github.com/join-com/avro-join'}}
const processApplicationStateReaderSchemaUpdated = {'reader':{'type':'record','name':'ProcessApplicationState','fields':[{'name':'applicationId','type':['null','int'],'default':null},{'name':'userId','type':['null','int'],'default':null}],'Event':'data-cmd-process-application-state','SchemaType':'READER','AvdlSchemaVersion':'4adb1df1c9243e24b937ddd165abf7572d7e2491','AvdlSchemaGitRemoteOriginUrl':'git@github.com:join-com/data.git','AvdlSchemaPathInGitRepo':'schemas/avro/commands/commands.avdl','GeneratorVersion':'387a0b3f2c890dc67f99085b7c94ff4bdc9cc967','GeneratorGitRemoteOriginUrl':'https://github.com/join-com/avro-join'}}

const processApplicationStateGCloudSchema = {
    type: 'AVRO',
    definition: processApplicationStateStringSchema
}

type ListSchemaAsyncIteratorMock = { [Symbol.asyncIterator](): AsyncIterableIterator<{ type: string, definition: string, name?:string }> }
const getPubsubMock = (asyncIterable: ListSchemaAsyncIteratorMock
                         = undefined as unknown as ListSchemaAsyncIteratorMock) => ({
    listSchemas: jest.fn().mockReturnValue(asyncIterable),
    createSchema: jest.fn(),
})
const getSchemaServiceClientMock = (revisions: unknown[] = []) => ({
    getSchema: jest.fn(),
    commitSchema: jest.fn(),
    listSchemaRevisions: jest.fn().mockResolvedValue( [revisions]),
    deleteSchemaRevision: jest.fn()
})

describe('SchemaDeployer.deployAvroSchemas', () => {
    beforeEach(() => {
        process.env['GCLOUD_PROJECT'] = 'project'
    })

    it('does nothing when no enabled avro topics', async () => {
        const schemasToDeploy = {
            'data-user-created': false,
            'data-user-deleted': false,
        }
        const schemaDeployer = new SchemaDeployer(loggerMock, getPubsubMock() as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)

        const { schemasCreated, revisionsCreated } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, {})

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(0)
    })

    it('throws error when non-avro schema exists with avro name', async () => {
        const nonAvroSchemaWithAvroName = {
            type: 'PROTOCOL_BUFFER',
            name: 'event-generated-avro',
            definition: 'some'
        }
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield nonAvroSchemaWithAvroName
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable)
        const schemaDeployer = new SchemaDeployer(loggerMock, pubsubMock as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)

        const schemasToDeploy = { 'pubsub-test-event': true }
        const readerSchemas = {'pubsub-test-event': processApplicationStateReaderSchema}

        await expect(schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)).rejects
          .toThrow('Non avro schema exists for avro topic \'event-generated-avro\', please remove it before starting the service')

    })

    it('does nothing when schemas fields match', async () => {
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield processApplicationStateGCloudSchema
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable)
        const schemaDeployer = new SchemaDeployer(loggerMock, pubsubMock as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)

        const schemasToDeploy = { 'data-cmd-process-application-state': true }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchema}

        const { schemasCreated, revisionsCreated } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(0)
    })

    it('creates schema when it doesn\'t exist', async () => {
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/no-empty-function
            async *[Symbol.asyncIterator]() {
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable)
        const schemaDeployer = new SchemaDeployer(loggerMock, pubsubMock as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)
        const schemasToDeploy = {
            'data-cmd-process-application-state': true,
        }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchema}

        const { schemasCreated, revisionsCreated } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(1)
        expect(revisionsCreated).toBe(0)
        expect(pubsubMock.createSchema).toHaveBeenCalledWith('data-cmd-process-application-state-generated-avro', 'AVRO', JSON.stringify(readerSchemas['data-cmd-process-application-state'].reader))
    })

    it('creates schema revision when schema fields don\'t match', async () => {
        const processApplicationStateGCloudSchema = {
            type: 'AVRO',
            definition: processApplicationStateStringSchema
        }
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield processApplicationStateGCloudSchema
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable)
        const schemaServiceClientMock = getSchemaServiceClientMock() as unknown as SchemaServiceClient
        const schemaDeployer = new SchemaDeployer(loggerMock, pubsubMock as unknown as PubSub,
          schemaServiceClientMock)
        const schemasToDeploy = {
            'data-cmd-process-application-state': true,
        }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchemaUpdated}

        const { schemasCreated, revisionsCreated } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(1)
        const schemaName = 'data-cmd-process-application-state' + SCHEMA_NAME_SUFFIX
        const schemaPath = `projects/${process.env['GCLOUD_PROJECT'] as string}/schemas/${schemaName}`
        expect(schemaServiceClientMock.commitSchema).toHaveBeenCalledWith({
            name: schemaPath, schema: {
                name: schemaPath, type: 'AVRO', definition: JSON.stringify(processApplicationStateReaderSchemaUpdated.reader),
            },
        })
    })

    it('does nothing when only revisionId changed and fields match', async () => {
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield processApplicationStateGCloudSchema
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable)
        const schemaDeployer = new SchemaDeployer(loggerMock, pubsubMock as unknown as PubSub,
          getSchemaServiceClientMock() as unknown as SchemaServiceClient)

        const schemasToDeploy = { 'data-cmd-process-application-state': true }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchemaOnlyCommitChanged}

        const { schemasCreated, revisionsCreated } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(0)
    })

    it('deletes old revision, when max number of revisions exist in the gcloud and creates new revision', async () => {
        const processApplicationStateGCloudSchema = {
            type: 'AVRO',
            definition: processApplicationStateStringSchema
        }
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield processApplicationStateGCloudSchema
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable)
        const revisions = new Array(MAX_REVISIONS_IN_GCLOUD)
        const revisionForDelete = {name: 'test'}
        revisions[MAX_REVISIONS_IN_GCLOUD - 1] = revisionForDelete
        const schemaServiceClientMock = getSchemaServiceClientMock(revisions) as unknown as SchemaServiceClient
        const schemaDeployer = new SchemaDeployer(loggerMock, pubsubMock as unknown as PubSub,
          schemaServiceClientMock)
        const schemasToDeploy = {
            'data-cmd-process-application-state': true,
        }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchemaUpdated}

        const { schemasCreated, revisionsCreated } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(1)
        const schemaName = 'data-cmd-process-application-state' + SCHEMA_NAME_SUFFIX
        const schemaPath = `projects/${process.env['GCLOUD_PROJECT'] as string}/schemas/${schemaName}`
        expect(schemaServiceClientMock.commitSchema).toHaveBeenCalledWith({
            name: schemaPath, schema: {
                name: schemaPath, type: 'AVRO', definition: JSON.stringify(processApplicationStateReaderSchemaUpdated.reader),
            },
        })
        expect(schemaServiceClientMock.deleteSchemaRevision).toHaveBeenCalledWith(revisionForDelete)
    })

    it('doesn\'t delete old revision, when max number of revisions is not reached and creates new revision', async () => {
        const processApplicationStateGCloudSchema = {
            type: 'AVRO',
            definition: processApplicationStateStringSchema
        }
        const asyncIterable = {
            // eslint-disable-next-line @typescript-eslint/require-await
            async *[Symbol.asyncIterator]() {
                yield processApplicationStateGCloudSchema
            }
        }
        const pubsubMock = getPubsubMock(asyncIterable)
        const revisions = new Array(MAX_REVISIONS_IN_GCLOUD - 1)
        const schemaServiceClientMock = getSchemaServiceClientMock(revisions) as unknown as SchemaServiceClient
        const schemaDeployer = new SchemaDeployer(loggerMock, pubsubMock as unknown as PubSub,
          schemaServiceClientMock)
        const schemasToDeploy = {
            'data-cmd-process-application-state': true,
        }
        const readerSchemas = {'data-cmd-process-application-state': processApplicationStateReaderSchemaUpdated}

        const { schemasCreated, revisionsCreated } = await schemaDeployer.deployAvroSchemas(schemasToDeploy, readerSchemas)

        expect(schemasCreated).toBe(0)
        expect(revisionsCreated).toBe(1)
        const schemaName = 'data-cmd-process-application-state' + SCHEMA_NAME_SUFFIX
        const schemaPath = `projects/${process.env['GCLOUD_PROJECT'] as string}/schemas/${schemaName}`
        expect(schemaServiceClientMock.commitSchema).toHaveBeenCalledWith({
            name: schemaPath, schema: {
                name: schemaPath, type: 'AVRO', definition: JSON.stringify(processApplicationStateReaderSchemaUpdated.reader),
            },
        })
        expect(schemaServiceClientMock.deleteSchemaRevision).not.toHaveBeenCalled()
    })
})
