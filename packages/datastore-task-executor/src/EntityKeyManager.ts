export type IKey = any
export type IGetKeyAttrs = [string, string]

export interface IEntityKeyManager {
  getKey: (id: string) => IKey
}

export interface IKeyClient {
  key: (attrs: IGetKeyAttrs) => IKey
}

export class EntityKeyManager implements IEntityKeyManager {
  constructor(readonly entity: string, readonly client: IKeyClient) {}

  public getKey(id: string): IKey {
    return this.client.key([this.entity, id])
  }
}
