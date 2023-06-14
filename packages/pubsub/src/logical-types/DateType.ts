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
    return date instanceof Date ? date.getTime() * 1000 : undefined
  }
  _resolve(type: Type) {
    if (Type.isType(type, 'logical:timestamp-micros')) {
      return this._fromValue
    }
    return
  }
}
