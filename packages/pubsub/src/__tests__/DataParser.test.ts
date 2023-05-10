import { DataParser } from '../DataParser'

describe('DataParser', () => {
  it('replaces null with undefined', () => {
    const obj = { a: { b: null }, arr: [{c: null}] }
    const parser = new DataParser()

    parser.replaceNullsWithUndefined(obj)

    expect(obj.a.b).toBeUndefined()
    expect(obj.arr[0]!.c).toBeUndefined()
  })

    it('leaves array of integer the same', () => {
      const arr = [1,2,3]
      const obj = { a: [...arr] }
      const parser = new DataParser()

      parser.replaceNullsWithUndefined(obj)

      expect(obj.a).toEqual(arr)
    })

    it('leaves array of dates the same', () => {
      const arr = [new Date(), new Date(), new Date()]
      const obj = { a: [...arr] }
      const parser = new DataParser()

      parser.replaceNullsWithUndefined(obj)

      expect(obj.a).toEqual(arr)
    })

    it('leaves date as property the same', () => {
      const date = new Date()
      const obj = { a: date }
      const parser = new DataParser()

      parser.replaceNullsWithUndefined(obj)

      expect(obj.a).toEqual(date)
    })
}
)
