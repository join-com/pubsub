export class FieldsProcessor {
  public findUndefinedOrNullOptionalArrays(obj: Record<string, unknown>, paths: string[]): string[] {
    const result: string[] = []
    for (const path of paths) {
      this.findUndefinedOrNullOptionalArraysRecursive(obj, path, path.split('.'), 0, result)
    }
    return result
  }

  public setEmptyArrayFieldsToUndefined(obj: Record<string, unknown>, paths: string[]) {
    for (const path of paths) {
      this.setEmptyArrayFieldsToUndefinedRecursive(obj, path.split('.'), 0)
    }
  }

  private setEmptyArrayFieldsToUndefinedRecursive(obj: Record<string, unknown>, splitPath: string[], fieldNumber: number) {
    if (!obj) {
      return
    }
    const key = splitPath[fieldNumber] as keyof typeof obj
    if (splitPath.length === fieldNumber + 1) {
      const arrayField = obj[key]
      if (!Array.isArray(obj[key]) || (Array.isArray(arrayField) && arrayField.length != 0)) {
        throw Error('Trying to replace existing data with undefined. Only empty array should be replaced with undefined.')
      }
      obj[key] = undefined
      return
    }
    const nextLevelObj = obj[key]
    if (nextLevelObj) {
      if (!Array.isArray(nextLevelObj)) {
        this.setEmptyArrayFieldsToUndefinedRecursive(nextLevelObj as Record<string, unknown>, splitPath, fieldNumber + 1)
      } else {
        for (const nextLevelObjectItem of nextLevelObj) {
          this.setEmptyArrayFieldsToUndefinedRecursive(nextLevelObjectItem as Record<string, unknown>, splitPath, fieldNumber + 1)
        }
      }
    }
  }

  private findUndefinedOrNullOptionalArraysRecursive(obj: Record<string, unknown>, path: string, splitPath: string[],
                                            fieldNumber: number, result: string[]) {
    if (!obj) {
      return
    }
    const key = splitPath[fieldNumber] as keyof typeof obj
    if (splitPath.length === fieldNumber + 1) {
      // undefined or null array found, add it to results
      if (!(obj[key])) {
        result.push(path)
      }
      return
    }
    const nextLevelObj = obj[key]
    if (nextLevelObj) {
      if (!Array.isArray(nextLevelObj)) {
        this.findUndefinedOrNullOptionalArraysRecursive(nextLevelObj as Record<string, unknown>, path, splitPath, fieldNumber + 1, result)
      } else {
        for (const nextLevelObjectItem of nextLevelObj) {
          this.findUndefinedOrNullOptionalArraysRecursive(nextLevelObjectItem as Record<string, unknown>, path, splitPath, fieldNumber + 1, result)
        }
      }
    }
  }
}
