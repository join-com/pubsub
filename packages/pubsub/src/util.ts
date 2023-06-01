export const replaceNullsWithUndefined = (obj: unknown) => {
  deepNullToUndefinedInObject(obj)
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
