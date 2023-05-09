import { DataParser } from '../DataParser'

describe('DataParser', () => {
  it('replaces null with undefined', () => {
    const obj = { a: { b: null }, arr: [{c: null}] }
    const parser = new DataParser()

    parser.replaceNullsWithUndefined(obj)

    expect(obj.a.b).toBeUndefined()
    expect(obj.arr[0]!.c).toBeUndefined()
  })
}
)
