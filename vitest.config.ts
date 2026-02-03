import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config'

/**
 * Default vitest configuration
 *
 * This config runs unit and conformance tests by default.
 * Integration and e2e tests are excluded because they require
 * the cloudflare:test environment with R2 bucket bindings.
 *
 * To run integration tests: npm run test:integration
 * To run e2e tests: npm run test:e2e
 */
export default defineWorkersConfig({
  test: {
    include: ['tests/unit/**/*.test.ts', 'tests/conformance/**/*.test.ts'],
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.test.toml' },
        miniflare: {
          compatibilityDate: '2025-01-29',
          compatibilityFlags: ['nodejs_compat'],
        },
      },
    },
  },
})
