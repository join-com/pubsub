const isObject = (obj: unknown): obj is Record<string, unknown> => {
  return obj instanceof Object && Object.keys(obj).length > 0
}
export class DataParser {

  public parse(data: Buffer): unknown {
    const dateTimeRegex = /^(\d{4}-\d\d-\d\d([tT][\d:.]*)?)([zZ]|([+-])(\d\d):?(\d\d))?$/
    const dateTimeReviver = (_: string, value: unknown) => {
      if (typeof value === 'string' && dateTimeRegex.test(value)) {
        return new Date(value)
      }
      return value
    }
    return JSON.parse(data.toString(), dateTimeReviver)
  }

  public replaceNullsWithUndefined<T>(obj: T) : T {
    if (isObject(obj)) {
      this.deepNullToUndefinedInObject(obj)
    }
    return obj
  }

  public deepNullToUndefinedInObject(obj: Record<string, unknown>)  {
    for (const key in obj) {
      const objProp = obj[key]
      if (objProp === null) {
        obj[key] = undefined
      } else if (Array.isArray(objProp)) {
        for (const member of objProp) {
          if (isObject(member)) {
            this.deepNullToUndefinedInObject(member)
          }
        }
      } else if (objProp instanceof Date || objProp instanceof Buffer) {
        continue
      } else if (isObject(objProp)) {
        this.deepNullToUndefinedInObject(objProp)
      }
    }
  }
}
