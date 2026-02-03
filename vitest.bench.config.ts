import { defineConfig } from 'vitest/config'

/**
 * Vitest benchmark configuration
 *
 * This config is specifically for running performance benchmarks.
 * Benchmarks use vitest's built-in bench() function for measuring
 * execution time of key Delta Lake operations.
 *
 * Run benchmarks with: npm run bench
 */
export default defineConfig({
  test: {
    include: ['benchmarks/**/*.bench.ts'],
    benchmark: {
      include: ['benchmarks/**/*.bench.ts'],
      // Report format: 'verbose' shows detailed statistics
      reporters: ['verbose'],
      // Output file for benchmark results (JSON format)
      outputFile: './benchmarks/results.json',
    },
  },
})
