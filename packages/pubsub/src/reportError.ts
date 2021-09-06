import { reportError as loggerReportError} from '@join-com/gcloud-logger-trace'

export const reportError = (e: unknown): void => {
  if (e instanceof Error) {
    loggerReportError(e)
  } else {
    loggerReportError(new Error('Unknown Error'))
  }
}
