module.exports = {
  runtime: '@side/jest-runtime',
  testEnvironment: 'node',
  setupFilesAfterEnv: ['jest-extended/all'],
  testPathIgnorePatterns: ['/node_modules/', 'generated', 'support'],
  watchPlugins: ['jest-watch-typeahead/filename', 'jest-watch-typeahead/testname'],
  workerIdleMemoryLimit: '1.5GB',
  transform: {
    '^.+\\.tsx?$': '@swc/jest',
  },
}
