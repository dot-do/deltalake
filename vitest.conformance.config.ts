import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['tests/conformance/**/*.test.ts'],
    // Use standard Node.js environment for conformance tests
    // (not Workers pool since we need filesystem access)
    pool: 'forks',
    testTimeout: 60000, // DAT tests may take longer due to file I/O
    hookTimeout: 30000,
  },
})
