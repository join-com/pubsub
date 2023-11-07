import { Type, types } from 'avsc'
import Field = types.Field
import RecordType = types.RecordType

export class AvroParser {

  public getOptionalArrayPaths(type: Type): string[] {
    const arraysPath: string[] = []
    if (!(type instanceof types.RecordType)) {
      throw Error('Top level type must be always record')
    }
    for (const field of type.fields) {
      this.findOptionalArrayPaths(field, [], arraysPath)
    }

    return arraysPath
  }

  private findOptionalArrayPaths(field: Field, path: string[], arraysPath: string[]) {
    let optional = false
    let type = field.type
    if (type instanceof types.UnwrappedUnionType) {
      const typeAfterNull = type.types[1]
      if (!typeAfterNull) {
        throw Error('Second type should be always a non-null type, as union used only for optional values')
      }
      type = typeAfterNull
      optional = true
    }

    if (type instanceof types.ArrayType) {
      if (optional) {
        let arrayPath = path.join('.')
        arrayPath = arrayPath ? arrayPath + '.' : ''
        arraysPath.push(arrayPath + field.name)
      }
      const itemsType = type.itemsType
      if (itemsType instanceof RecordType) {
        for (const internalField of itemsType.fields) {
          path.push(field.name)
          this.findOptionalArrayPaths(internalField, path, arraysPath)
          path.pop()
        }
      }
    }

    if (type instanceof types.RecordType) {
      for (const internalField of type.fields) {
        path.push(field.name)
        this.findOptionalArrayPaths(internalField, path, arraysPath)
        path.pop()
      }
    }
  }
}
