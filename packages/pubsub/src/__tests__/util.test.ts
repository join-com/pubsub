import { ILogger } from '../ILogger'
import { includesUndefined, logWarnWhenUndefinedInNullPreserveFields, replaceNullsWithUndefined } from '../util'

describe('Null to undefined', () => {
    it('replaces null with undefined', () => {
      const obj = { a: { b: null }, arr: [{ c: null }] }

      replaceNullsWithUndefined(obj)

      expect(obj.a.b).toBeUndefined()
      expect(obj.arr[0]!.c).toBeUndefined()
    })

    it('leaves array of integer the same', () => {
      const arr = [1, 2, 3]
      const obj = { a: [...arr] }

      replaceNullsWithUndefined(obj)

      expect(obj.a).toEqual(arr)
    })

    it('leaves array of dates the same', () => {
      const arr = [new Date(), new Date(), new Date()]
      const obj = { a: [...arr] }

      replaceNullsWithUndefined(obj)

      expect(obj.a).toEqual(arr)
    })

    it('leaves date as property not changed', () => {
      const date = new Date()
      const obj = { a: date }

      replaceNullsWithUndefined(obj)

      expect(obj.a).toEqual(date)
    })
  },
)

describe('includesUndefinedCheck', () => {
    it('returns true if undefined inside', () => {
      const obj = { a: { b: null }, arr: [{ c: undefined }] }
      expect(includesUndefined(obj)).toBeTrue()
    })

    it('returns false if no undefined inside', () => {
      const obj = { a: { b: null }, arr: [{ c: null }] }
      expect(includesUndefined(obj)).toBeFalse()
    })
  },
)

describe('logWarnWhenUndefinedInNullPreserveFields', () => {
    const consoleMock = {
      warn: jest.fn(),
    }

    afterEach(() => {
      consoleMock.warn.mockReset()
    })

    it('logs warn when undefined is inside NullPreserve fields', () => {
      const obj = { a: { b: null }, arr: [{ c: undefined }] }
      logWarnWhenUndefinedInNullPreserveFields(obj, 'someField, arr', consoleMock as unknown as ILogger)
      expect(consoleMock.warn).toHaveBeenCalledOnce()
    })

    it('doesn\'t log warn when null is inside NullPreserve fields', () => {
      const obj = { a: { b: null }, arr: [{ c: null }] }
      logWarnWhenUndefinedInNullPreserveFields(obj, 'someField, arr', consoleMock as unknown as ILogger)
      expect(consoleMock.warn).not.toHaveBeenCalled()
    })

    it('doesn\'t log warn when undefined is inside non NullPreserve fields', () => {
      const obj = { a: { b: null }, arr: [{ c: undefined }] }
      logWarnWhenUndefinedInNullPreserveFields(obj, 'someField', consoleMock as unknown as ILogger)
      expect(consoleMock.warn).not.toHaveBeenCalled()
    })
  },
)
