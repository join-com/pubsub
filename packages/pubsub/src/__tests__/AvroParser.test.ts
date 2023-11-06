import { Schema, Type } from 'avsc'
import { AvroParser } from '../AvroParser'
import { DateType } from '../logical-types/DateType'
import { SCHEMA_DEFINITION_EXAMPLE } from './support/constants'


describe('AvroParser', () => {
  it('returns no fields, when no arrays inside the schema', () => {
    const type = Type.forSchema(SCHEMA_DEFINITION_EXAMPLE as Schema, { logicalTypes: { 'timestamp-micros': DateType } })
    const avroParser = new AvroParser()
    const arrayPaths = avroParser.getOptionalArrayPaths(type)
    expect(arrayPaths).toBeEmpty()
  })

  it('returns no fields when mandatory array is inside the schema', () => {
    const schemaWithOneArrayField = {
      'type': 'record',
      'name': 'Avro',
      'fields': [
        {
          'name': 'tags',
          'type': {
            'type': 'array',
            'items': 'string',
          },
          'default': [],
        },
      ],
    }
    const type = Type.forSchema(schemaWithOneArrayField as Schema, { logicalTypes: { 'timestamp-micros': DateType } })
    const avroParser = new AvroParser()
    const arrayPaths = avroParser.getOptionalArrayPaths(type)
    expect(arrayPaths).toBeEmpty()
  })

  it('returns no fields when nested mandatory array is inside the schema', () => {
    const schemaWithOneNestedArrayField = {
      'type': 'record',
      'name': 'Avro',
      'fields': [
        {
          'name': 'user',
          'type': {
            'type': 'record',
            'name': 'user',
            'fields': [
              {
                'name': 'tags',
                'type': {
                  'type': 'array',
                  'items': 'string',
                },
                'default': [],
              },
            ],
          },
        },
      ],
    }
    const type = Type.forSchema(schemaWithOneNestedArrayField as Schema, { logicalTypes: { 'timestamp-micros': DateType } })
    const avroParser = new AvroParser()
    const arrayPaths = avroParser.getOptionalArrayPaths(type)
    expect(arrayPaths).toBeEmpty()
  })

  it('returns one field when optional array is inside the schema', () => {
    const schemaWithOneOptionalArrayField = {
      'type': 'record',
      'name': 'Avro',
      'fields': [
        {
          'name': 'tags',
          'type': [
            'null',
            {
              'type': 'array',
              'items': 'string'
            }
          ],
        }
      ]
    }
    const type = Type.forSchema(schemaWithOneOptionalArrayField as Schema, { logicalTypes: { 'timestamp-micros': DateType } })
    const avroParser = new AvroParser()
    const arrayPaths = avroParser.getOptionalArrayPaths(type)
    expect(arrayPaths).toEqual(['tags'])
  })

  it('returns one field when optional nested array is inside the schema', () => {
    const schemaWithOneNestedOptionalArrayField = {
      'type': 'record',
      'name': 'Avro',
      'fields': [
        {
          'name': 'user',
          'type': {
            'type': 'record',
            'name': 'user',
            'fields': [
              {
                'name': 'tags',
                'type': [
                  'null',
                  {
                    'type': 'array',
                    'items': 'string'
                  }
                ],
              },
            ],
          },
        },
      ],
    }
    const type = Type.forSchema(schemaWithOneNestedOptionalArrayField as Schema, { logicalTypes: { 'timestamp-micros': DateType } })
    const avroParser = new AvroParser()
    const arrayPaths = avroParser.getOptionalArrayPaths(type)
    expect(arrayPaths).toEqual(['user.tags'])
  })

  it('returns two fields when optional array and optional nested array is inside the schema', () => {
    const schemaWithOneNestedOptionalArrayField = {
      'type': 'record',
      'name': 'Avro',
      'fields': [
        {
          'name': 'user',
          'type': {
            'type': 'record',
            'name': 'user',
            'fields': [
              {
                'name': 'tags',
                'type': [
                  'null',
                  {
                    'type': 'array',
                    'items': 'string'
                  }
                ],
              },
            ],
          },
        },
        {
          'name': 'names',
          'type': [
            'null',
            {
              'type': 'array',
              'items': 'string'
            }
          ],
        }
      ],
    }
    const type = Type.forSchema(schemaWithOneNestedOptionalArrayField as Schema, { logicalTypes: { 'timestamp-micros': DateType } })
    const avroParser = new AvroParser()
    const arrayPaths = avroParser.getOptionalArrayPaths(type)
    expect(arrayPaths).toEqual(['user.tags', 'names'])
  })

  it('returns one field when optional double nested array is inside the schema', () => {
    const schemaWithOneDoubleNestedOptionalArrayField = {
      'type': 'record',
      'name': 'Avro',
      'fields': [
        {
          'name': 'user',
          'type': {
            'type': 'record',
            'name': 'user',
            'fields': [
              {
                'name': 'document',
                'type': {
                  'type': 'record',
                  'fields': [
                    {
                      'name': 'tags',
                      'type': [
                        'null',
                        {
                          'type': 'array',
                          'items': 'string'
                        }
                      ],
                    },
                  ]
                },
              }
            ],
          },
        },
      ],
    }
    const type = Type.forSchema(schemaWithOneDoubleNestedOptionalArrayField as Schema, { logicalTypes: { 'timestamp-micros': DateType } })
    const avroParser = new AvroParser()
    const arrayPaths = avroParser.getOptionalArrayPaths(type)
    expect(arrayPaths).toEqual(['user.document.tags'])
  })

  it('returns one field when <optional array is inside record that is inside array> is inside the schema', () => {
    const schemaWithOptionalArrayInsideRecordInsideArrayField = {
      'type': 'record',
      'name': 'Avro',
      'fields': [
        {
          'name': 'users',
          'type': {
            'type': 'array',
            'items': {
              'type': 'record',
              'name': 'User',
              'fields': [
                {
                  'name': 'document',
                  'type': {
                    'type': 'record',
                    'fields': [
                      {
                        'name': 'tags',
                        'type': [
                          'null',
                          {
                            'type': 'array',
                            'items': 'string'
                          }
                        ],
                      },
                    ]
                  },
                }
              ],
            },
          },

        },
      ],
    }
    const type = Type.forSchema(schemaWithOptionalArrayInsideRecordInsideArrayField as Schema, { logicalTypes: { 'timestamp-micros': DateType } })
    const avroParser = new AvroParser()
    const arrayPaths = avroParser.getOptionalArrayPaths(type)
    expect(arrayPaths).toEqual(['users.document.tags'])
  })
})
