import { includesUndefined, logWarnWhenUndefinedInNullPreserveFields } from '../../deprecated/preserveNullUtil'
import { ILogger } from '../../ILogger'

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
