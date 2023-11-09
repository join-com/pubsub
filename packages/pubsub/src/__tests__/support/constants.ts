import { ISchema } from '@google-cloud/pubsub'

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
      'name': 'createdAt',
      'type': {type: 'long', logicalType: 'timestamp-micros'}
    },
    {
      'name': 'third',
      'type': [
        'null',
        'string'
      ],
      'default': null
    },
    {
      'name': 'fourth',
      'type': ['null', {
        'type': 'record',
        'name': 'nestedEntity',
        'fields': [{
          'name': 'flag',
          'type': ['null', 'boolean'],
          'default': null,
        }],
      }],
      'default': null,
    }
  ],
  'Event' : 'pubsub-test-event',
  'GeneratorVersion' : '1.0.0',
  'GeneratorGitRemoteOriginUrl' : 'git@github.com:join-com/avro-join.git',
  'SchemaType' : 'WRITER',
  'AvdlSchemaGitRemoteOriginUrl' : 'git@github.com:join-com/data.git',
  'AvdlSchemaPathInGitRepo' : 'src/test/resources/input.avdl',
  'AvdlSchemaVersion': 'commit-hash'
}

export const SCHEMA_DEFINITION_PRESERVE_NULL_EXAMPLE = {
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
      'name': 'createdAt',
      'type': {type: 'long', logicalType: 'timestamp-micros'}
    },
    {
      'name': 'third',
      'type': [
        'null',
        'string'
      ],
      'default': null
    },
    {
      'name' : 'now',
      'type' : [ 'null', {
        'type' : 'record',
        'name' : 'Recruiter',
        'fields' : [ {
          'name' : 'id',
          'type' : [ 'null', 'int' ],
          'default' : null
        }, {
          'name' : 'firstName',
          'type' : [ 'null', 'string' ],
          'default' : null
        }, ]
      } ],
      'default' : null
    },
  ],
  'Event' : 'pubsub-test-event',
  'GeneratorVersion' : '1.0.0',
  'GeneratorGitRemoteOriginUrl' : 'git@github.com:join-com/avro-join.git',
  'SchemaType' : 'WRITER',
  'AvdlSchemaGitRemoteOriginUrl' : 'git@github.com:join-com/data.git',
  'AvdlSchemaPathInGitRepo' : 'src/test/resources/input.avdl',
  'AvdlSchemaVersion': 'commit-hash'
}

export const SCHEMA_DEFINITION_READER_OPTIONAL_ARRAY_EXAMPLE = {
  'type': 'record',
  'name': 'Avro',
  'fields': [
    {
      'name': 'first',
      'type': 'string'
    },
    {
      'name': 'tags',
      'type': [
        {
          'type': 'array',
          'items': 'string'
        },
      ],
      'default': []
    },
    {
      'name': 'languages',
      'type': [
        {
          'type': 'array',
          'items': 'string'
        }
      ],
      'default': []
    }
  ],
  'Event' : 'pubsub-test-event',
  'GeneratorVersion' : '1.0.0',
  'GeneratorGitRemoteOriginUrl' : 'git@github.com:join-com/avro-join.git',
  'SchemaType' : 'WRITER',
  'AvdlSchemaGitRemoteOriginUrl' : 'git@github.com:join-com/data.git',
  'AvdlSchemaPathInGitRepo' : 'src/test/resources/input.avdl',
  'AvdlSchemaVersion': 'commit-hash'
}

export const SCHEMA_DEFINITION_WRITER_OPTIONAL_ARRAY_EXAMPLE = {
  'type': 'record',
  'name': 'Avro',
  'fields': [
    {
      'name': 'first',
      'type': 'string'
    },
    {
      'name': 'tags',
      'type': [
        'null',
        {
          'type': 'array',
          'items': 'string'
        }
      ],
      'default': null
    },
    {
      'name': 'languages',
      'type': [
        'null',
        {
          'type': 'array',
          'items': 'string'
        },
      ],
      'default': null
    }
  ],
  'Event' : 'pubsub-test-event',
  'GeneratorVersion' : '1.0.0',
  'GeneratorGitRemoteOriginUrl' : 'git@github.com:join-com/avro-join.git',
  'SchemaType' : 'WRITER',
  'OptionalArrayPaths' : 'tags,languages',
  'AvdlSchemaGitRemoteOriginUrl' : 'git@github.com:join-com/data.git',
  'AvdlSchemaPathInGitRepo' : 'src/test/resources/input.avdl',
  'AvdlSchemaVersion': 'commit-hash'
}
export const SCHEMA_EXAMPLE: ISchema = {definition: JSON.stringify(SCHEMA_DEFINITION_EXAMPLE), revisionId: 'example'}
