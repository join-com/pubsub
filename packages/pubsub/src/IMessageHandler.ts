export interface IMessageHandler {
  initialize: () => Promise<void>
  start: () => void
  stop: () => Promise<void>
}
