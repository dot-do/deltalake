import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config'

export default defineWorkersConfig({
  test: {
    include: ['tests/integration/**/*.test.ts'],
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
