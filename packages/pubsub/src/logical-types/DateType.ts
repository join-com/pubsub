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
