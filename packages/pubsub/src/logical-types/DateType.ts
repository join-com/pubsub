/* eslint-disable */
import { Type, types } from 'avsc'

/**
 * Custom logical type used to encode native Date objects as longs.
 *
 * It also supports reading dates serialized as strings (by creating an
 * appropriate resolver).
 *
 */
export class DateType extends types.LogicalType {
  _fromValue(val : any) {
    return new Date(val / 1000)
  }
  _toValue(date: any) {
    if (!(date instanceof Date)) {
      return undefined
    }
    let dateInMillis = date.getTime() * 1000
    // If number is not a safe integer, it will lose precision during conversion:
    // https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/MAX_SAFE_INTEGER
    // Avsc will throw errors trying to convert number larger than Number.MAX_SAFE_INTEGER - 1
    // Only possibility to fix is to use custom long types like BigInt, but still it will not work for json conversion,
    // because of that limiting date to max possible date in micros, if received value larger than that
    // https://github.com/mtth/avsc/wiki/Advanced-usage#custom-long-types
    if (Number.isSafeInteger(dateInMillis)) {
      return dateInMillis
    } else {
      // max safe date in micros is "2255-06-05T23:47:34.740Z"
      return Number.MAX_SAFE_INTEGER - 1
    }
  }
  _resolve(type: Type) {
    if (Type.isType(type, 'logical:timestamp-micros')) {
      return this._fromValue
    }
    return
  }
}
