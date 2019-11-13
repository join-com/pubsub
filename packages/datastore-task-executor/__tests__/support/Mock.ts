export type Mock<T> = {
  [P in keyof T]: T[P] & jest.Mock<any, any>
}
