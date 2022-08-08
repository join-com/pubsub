import { CallOptions } from '@google-cloud/pubsub'

export const createCallOptions: CallOptions = {
  retry: {
    backoffSettings: {
      maxRetries: 5,
      initialRetryDelayMillis: 1000,
      retryDelayMultiplier: 2,
      maxRetryDelayMillis: 10000,
    },
  },
}
