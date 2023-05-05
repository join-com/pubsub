export class DataParser {
  public parse(data: string): unknown {
    const dateTimeRegex = /^(\d{4}-\d\d-\d\d([tT][\d:.]*)?)([zZ]|([+-])(\d\d):?(\d\d))?$/
    const dateTimeReviver = (_: string, value: unknown) => {
      if (typeof value === 'string' && dateTimeRegex.test(value)) {
        return new Date(value)
      }
      return value
    }
    return JSON.parse(data, dateTimeReviver)
  }
}
