import { ILogger } from './ILogger'

export const replaceNullsWithUndefined = (obj: unknown, preserveNullFields?: string) => {
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

export const logWarnWhenUndefinedInNullPreserveFields = (obj: unknown, nullPreserveFields: string, logger?: ILogger) => {
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

export function includesUndefined(obj: unknown) {
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

const deepNullToUndefinedInObject = (obj: unknown): unknown => {
  if (obj === null || obj === undefined) {
    return undefined
  }
  if (!(obj instanceof Object) || obj instanceof Date || obj instanceof Buffer) {
    return obj
  }
  if (Array.isArray(obj)) {
    return obj.map(v => deepNullToUndefinedInObject(v))
  }
  if (isObject(obj)) {
    for (const key of Object.keys(obj)) {
      obj[key] = deepNullToUndefinedInObject(obj[key])
    }
  }
  return obj
}
const isObject = (obj: unknown): obj is Record<string, unknown> => {
  return obj instanceof Object && Object.keys(obj).length > 0
}

function parsePreserveNullFields(nullPreserveFields: string) {
  return nullPreserveFields.split(',').map(v => v.trim())
}
