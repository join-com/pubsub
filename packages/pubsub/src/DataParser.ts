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
    this.deepNullToUndefinedInObject(obj as Record<string, unknown>)
    return obj
  }

  private deepNullToUndefinedInObject(obj: Record<string, unknown>)  {
    for (const key in obj) {
      if (obj[key] === null) {
        obj[key] = undefined
      } else if (Array.isArray(obj[key])) {
        for (const member of obj[key] as Array<unknown>) {
          if (typeof member === 'object' && Object.keys(member as object).length > 0) {
            this.deepNullToUndefinedInObject(member as Record<string, unknown>)
          }
        }
      } else if (typeof obj[key] === 'object' && Object.keys(obj[key] as object).length > 0) {
        this.deepNullToUndefinedInObject(obj[key] as Record<string, unknown>)
      }
    }
  }
}




