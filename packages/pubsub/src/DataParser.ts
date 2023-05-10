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
    this.deepReplaceInObject(obj as Record<string, unknown>)
    return obj
  }

  private deepReplaceInObject(obj: Record<string, unknown>)  {
    for (const key in obj) {
      if (obj[key] === null) {
        obj[key] = undefined
      } else if (Array.isArray(obj[key])) {
        (obj[key] as Record<string, unknown>[]).forEach(member => this.deepReplaceInObject(member))
      } else if (typeof obj[key] === 'object') {
        this.deepReplaceInObject(obj[key] as Record<string, unknown>)
      }
    }
  }
}
