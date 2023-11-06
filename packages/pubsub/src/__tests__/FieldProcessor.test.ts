import { FieldsProcessor } from '../FieldsProcessor'


describe('FieldsProcessor', () => {
  const fieldsProcessor = new FieldsProcessor()

  describe('findUndefinedOrNullOptionalArrays', () => {
    it('finds undefined array on the first level', () => {
      const data = {
        'array': undefined
      }

      const optionalArrays = fieldsProcessor.findUndefinedOrNullOptionalArrays(data, ['array'])

      expect(optionalArrays).toEqual(['array'])
    })

    it('finds null array on the first level', () => {
      const data = {
        'array': null
      }

      const optionalArrays = fieldsProcessor.findUndefinedOrNullOptionalArrays(data, ['array'])

      expect(optionalArrays).toEqual(['array'])
    })

    it('skips defined arrays on the first level', () => {
      const data = {
        'array': []
      }

      const optionalArrays = fieldsProcessor.findUndefinedOrNullOptionalArrays(data, ['array'])

      expect(optionalArrays).toEqual([])
    })

    it('finds undefined array on the second level', () => {
      const data = {
        entity: {
          'array': undefined
        }
      }

      const optionalArrays = fieldsProcessor.findUndefinedOrNullOptionalArrays(data, ['entity.array'])

      expect(optionalArrays).toEqual(['entity.array'])
    })

    it('finds undefined array on the second level inside arrays', () => {
      const data = {
        entities: [{
          'array': undefined
        }]
      }

      const optionalArrays = fieldsProcessor.findUndefinedOrNullOptionalArrays(data, ['entities.array'])

      expect(optionalArrays).toEqual(['entities.array'])
    })

    it('ignores undefined paths on the first two levels', () => {
      const data = {
        entities: [{
          'array': undefined
        }]
      }

      const optionalArrays = fieldsProcessor.findUndefinedOrNullOptionalArrays(data, ['entities.array',
        'entities.nonexistent.array', 'nonexistent.array'])

      expect(optionalArrays).toEqual(['entities.array'])
    })

    it('finds undefined array on the third level', () => {
      const data = {
        'topLevel': {
          entity: {
            'array': undefined
          }
        }
      }

      const optionalArrays = fieldsProcessor.findUndefinedOrNullOptionalArrays(data, ['topLevel.entity.array'])

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

    it('sets second level field inside array to undefined', () => {
      const data = {
        'entities': [{
          'array': []
        }]
      }

      fieldsProcessor.setEmptyArrayFieldsToUndefined(data, ['entities.array'])
      expect(data.entities[0]).toBeDefined()
      expect(data.entities[0]?.array).toBeUndefined()
    })

    it('sets third level field inside array to undefined', () => {
      const data = {
        'topLevel': {
          'entities': [{
            'array': []
          }]
        }
      }

      fieldsProcessor.setEmptyArrayFieldsToUndefined(data, ['topLevel.entities.array'])
      expect(data.topLevel.entities[0]).toBeDefined()
      expect(data.topLevel.entities[0]?.array).toBeUndefined()
    })
  })
})
