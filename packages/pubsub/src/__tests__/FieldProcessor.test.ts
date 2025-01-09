import { FieldsProcessor } from '../FieldsProcessor'


describe('FieldsProcessor', () => {
  const fieldsProcessor = new FieldsProcessor()

  describe('findAndReplaceUndefinedOrNullOptionalArrays', () => {
    it('finds and replaces undefined array on the first level', () => {
      const data = {
        'array': undefined
      }

      const optionalArrays = fieldsProcessor.findAndReplaceUndefinedOrNullOptionalArrays(data, ['array'])

      expect(data.array).toBeEmpty()
      expect(optionalArrays).toEqual(['array'])
    })

    it('finds and replaces null array on the first level', () => {
      const data = {
        'array': null
      }

      const optionalArrays = fieldsProcessor.findAndReplaceUndefinedOrNullOptionalArrays(data, ['array'])

      expect(data.array).toBeEmpty()
      expect(optionalArrays).toEqual(['array'])
    })

    it('skips defined arrays on the first level', () => {
      const data = {
        'array': []
      }

      const optionalArrays = fieldsProcessor.findAndReplaceUndefinedOrNullOptionalArrays(data, ['array'])

      expect(optionalArrays).toEqual([])
    })

    it('finds and replaces undefined array on the second level', () => {
      const data = {
        entity: {
          'array': undefined
        }
      }

      const optionalArrays = fieldsProcessor.findAndReplaceUndefinedOrNullOptionalArrays(data, ['entity.array'])

      expect(data.entity.array).toBeEmpty()
      expect(optionalArrays).toEqual(['entity.array'])
    })

    it('replaces undefined array on the second level inside arrays, but do not save it for replacing back', () => {
      const data = {
        entities: [{
          'array': undefined
        }]
      }

      const optionalArrays = fieldsProcessor.findAndReplaceUndefinedOrNullOptionalArrays(data, ['entities.array'])

      expect(data.entities[0]).toBeDefined()
      expect(data.entities[0]?.array).toBeEmpty()

      expect(optionalArrays).toEqual([])
    })

    it('ignores undefined paths on the first two levels', () => {
      const data = {
        entities: [{
          'array': null
        }]
      }

      const optionalArrays = fieldsProcessor.findAndReplaceUndefinedOrNullOptionalArrays(data, ['entities.array',
        'entities.nonexistent.array', 'nonexistent.array'])

      expect(data.entities[0]).toBeDefined()
      expect(data.entities[0]?.array).toBeEmpty()
      expect(optionalArrays).toEqual([])
    })

    it('finds and replaces undefined array on the third level', () => {
      const data = {
        'topLevel': {
          entity: {
            'array': undefined
          }
        }
      }

      const optionalArrays = fieldsProcessor.findAndReplaceUndefinedOrNullOptionalArrays(data, ['topLevel.entity.array'])

      expect(data.topLevel.entity.array).toBeEmpty()
      expect(optionalArrays).toEqual(['topLevel.entity.array'])
    })
  })

  describe('setFieldsToUndefined', () => {
    it('sets top level field to undefined', () => {
      const data = {
        'array': []
      }

      fieldsProcessor.setEmptyArrayFieldsToUndefined(data, ['array'])

      expect(data.array).toBeUndefined()
    })

    it('throws when tries to replace non empty array', () => {
      const data = {
        'array': [1]
      }

      expect(() => fieldsProcessor.setEmptyArrayFieldsToUndefined(data, ['array'])).toThrow()
    })

    it('sets second level field to undefined', () => {
      const data = {
        entity: {
          'array': []
        }
      }

      fieldsProcessor.setEmptyArrayFieldsToUndefined(data, ['entity.array'])

      expect(data.entity.array).toBeUndefined()
    })

    it('does not set second level field inside array to undefined', () => {
      const data = {
        'entities': [{
          'array': []
        }]
      }

      fieldsProcessor.setEmptyArrayFieldsToUndefined(data, ['entities.array'])
      expect(data.entities[0]).toBeDefined()
      expect(data.entities[0]?.array).toBeEmpty()
    })

    it('does not set third level field inside array to undefined', () => {
      const data = {
        'topLevel': {
          'entities': [{
            'array': []
          }]
        }
      }

      fieldsProcessor.setEmptyArrayFieldsToUndefined(data, ['topLevel.entities.array'])
      expect(data.topLevel.entities[0]).toBeDefined()
      expect(data.topLevel.entities[0]?.array).toBeEmpty()
    })

    it('skips setting field inside array to undefined', () => {
      const data = {
        screeningQuestions: [
          {
            type: 'TEXT',
            options: []
          },
          {
            type: 'SINGLE_CHOICE',
            options: ['Option 1', 'Option 2'],
          },
        ],
      }

      fieldsProcessor.setEmptyArrayFieldsToUndefined(data, ['screeningQuestions.options'])
      expect(data.screeningQuestions[0]).toBeDefined()
      expect(data.screeningQuestions[0]?.options).toBeEmpty()
      expect(data.screeningQuestions[1]?.options).toHaveLength(2)
    })
  })
})
