/**
 * FieldsProcessor is a utility class to process fields in an object.
 */
export class FieldsProcessor {
  /**
   * As we left possibility for developers to make array optional, but in BigQuery arrays can not be optional,
   * we need to replace undefined or null arrays with empty arrays. For this during avro generation we saved paths
   * where we have arrays, and we are going through them and replacing undefined or null with empty arrays.
   * We are also remembering which field we changed, saving it in metadata, and setting undefined back.
   * But if we have structure like array -> object -> array, we are not modifying nested array back, because it is
   * not known at which index it was undefined.
   * @param obj object in which we need to replace undefined or null arrays
   * @param paths paths where we have arrays inside the object
   */
  public findAndReplaceUndefinedOrNullOptionalArrays(obj: Record<string, unknown>, paths: string[]): string[] {
    const result: string[] = []
    for (const path of paths) {
      this.findAndReplaceUndefinedOrNullOptionalArraysRecursive(obj, path, path.split('.'), 0, result)
    }
    return result
  }

  /**
   * Replaces empty arrays with undefined in the object.
   * @param obj object in which we need to replace empty arrays
   * @param paths paths where we have empty arrays inside the object
   */
  public setEmptyArrayFieldsToUndefined(obj: Record<string, unknown>, paths: string[]): void {
    for (const path of paths) {
      this.setEmptyArrayFieldsToUndefinedRecursive(obj, path.split('.'), 0)
    }
  }

  private findAndReplaceUndefinedOrNullOptionalArraysRecursive(obj: Record<string, unknown>, path: string, splitPath: string[],
                                                     fieldNumber: number, result: string[], insideArray = false) {
    if (!obj) {
      return
    }
    const key = splitPath[fieldNumber] as keyof typeof obj
    if (splitPath.length === fieldNumber + 1) {
      // undefined or null array found, add it to results
      if (!(obj[key])) {
        // as we are not remembering for which index inside array we set undefined, we can replace it back only if it is not inside array
        if (!insideArray) {
          result.push(path)
        }
        obj[key] = []
      }
      return
    }
    const nextLevelObj = obj[key]
    if (nextLevelObj) {
      if (!Array.isArray(nextLevelObj)) {
        this.findAndReplaceUndefinedOrNullOptionalArraysRecursive(nextLevelObj as Record<string, unknown>, path, splitPath, fieldNumber + 1, result)
      } else {
        for (const nextLevelObjectItem of nextLevelObj) {
          this.findAndReplaceUndefinedOrNullOptionalArraysRecursive(nextLevelObjectItem as Record<string, unknown>, path, splitPath, fieldNumber + 1, result, true)
        }
      }
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
        throw new Error('Trying to replace existing data with undefined. Only empty array should be replaced with undefined.')
      }
      obj[key] = undefined
      return
    }
    const nextLevelObj = obj[key]
    if (nextLevelObj) {
      // we are not setting undefined for nested arrays, as we do not know at which index it was empty
      if (!Array.isArray(nextLevelObj)) {
        this.setEmptyArrayFieldsToUndefinedRecursive(nextLevelObj as Record<string, unknown>, splitPath, fieldNumber + 1)
      }
    }
  }
}
