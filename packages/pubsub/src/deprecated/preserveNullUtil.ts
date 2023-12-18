import { ILogger } from '../ILogger'
import { deepNullToUndefinedInObject, isObject } from '../util'

/**
 * @deprecated should be only used inside deprecated PreserveNull annotation
 */
export const replaceNullsWithUndefined = (obj: unknown, preserveNullFields?: string): void => {
  if (!preserveNullFields) {
    deepNullToUndefinedInObject(obj)
  } else {
    if (!isObject(obj)) {
      return
    }
    const preserveNullFieldsList = parsePreserveNullFields(preserveNullFields)
    for (const key of Object.keys(obj)) {
      if (!preserveNullFieldsList.includes(key)) {
        obj[key] = deepNullToUndefinedInObject(obj[key])
      }
    }
  }
}

/**
 * @deprecated should be only used inside deprecated PreserveNull annotation
 */
export const logWarnWhenUndefinedInNullPreserveFields = (obj: unknown, nullPreserveFields: string, logger?: ILogger): void => {
  if (isObject(obj)) {
    const fieldsWithOnlyNullInside = parsePreserveNullFields(nullPreserveFields)
    for (const key of Object.keys(obj)) {
      if (fieldsWithOnlyNullInside.includes(key)) {
        if (includesUndefined(obj[key])) {
          logger?.warn(`Message contains undefined in one of the NullPreserveFields: ${nullPreserveFields}`, obj)
        }
      }
    }
  }
}

/**
 * @deprecated is used inside deprecated PreserveNull annotation
 */
export function includesUndefined(obj: unknown): boolean {
  if (obj === undefined) {
    return true;
  }
  if (obj === null || !(obj instanceof Object) || obj instanceof Date || obj instanceof Buffer) {
    return false
  }
  if (Array.isArray(obj)) {
    for (const element of obj) {
      if (includesUndefined(element)) {
        return true;
      }
    }
  }
  if (isObject(obj)) {
    for (const key of Object.keys(obj)) {
      if (includesUndefined(obj[key])) {
        return true;
      }
    }
  }
  return false;
}

function parsePreserveNullFields(nullPreserveFields: string) {
  return nullPreserveFields.split(',').map(v => v.trim())
}
