export class DataParser {
  public parse(data: Buffer) {
    const dateTimeRegex =
      /^(\d{4}\-\d\d\-\d\d([tT][\d:\.]*)?)([zZ]|([+\-])(\d\d):?(\d\d))?$/
    const dateTimeReviver = (_: string, value: any) => {
      if (typeof value === 'string' && dateTimeRegex.test(value)) {
        return new Date(value)
      }
      return value
    }
    return JSON.parse(data.toString(), dateTimeReviver)
  }
}
