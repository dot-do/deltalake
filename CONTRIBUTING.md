# Contributing to @dotdo/deltalake

Thank you for your interest in contributing to `@dotdo/deltalake`. This guide will help you get started.

## Development Setup

### Prerequisites

- Node.js 18.0.0 or higher
- npm

### Getting Started

```bash
# Clone the repository
git clone https://github.com/dot-do/deltalake.git
cd deltalake

# Install dependencies
npm install

# Build the project
npm run build

# Run tests to verify setup
npm test
```

## Running Tests

The project uses Vitest with different configurations for different test types:

```bash
# Run all tests
npm test

# Run unit tests (uses vitest-pool-workers with miniflare)
npm run test:unit

# Run unit tests with coverage
npm run test:coverage

# Run integration tests (multi-worker miniflare)
npm run test:integration

# Run end-to-end tests (against deployed service)
npm run test:e2e

# Run conformance tests (Delta Lake specification compliance)
npm run test:conformance
```

Unit tests run in a Cloudflare Workers-compatible environment via miniflare to ensure edge runtime compatibility.

## Code Style Guidelines

### TypeScript

- Use strict TypeScript (`strict: true` is enabled)
- All code must pass type checking: `npm run typecheck`
- Prefer explicit types over `any`
- Use `noUncheckedIndexedAccess` - handle undefined values from array/object access

### Project Structure

```
src/
├── storage/    # Storage backend implementations (R2, S3, filesystem, memory)
├── parquet/    # Parquet utilities (streaming writer, variant encoding)
├── query/      # MongoDB-style query operators with predicate pushdown
├── cdc/        # Change data capture primitives
├── delta/      # Delta Lake transaction log
└── compaction/ # Table compaction utilities
```

### Naming Conventions

- Use camelCase for variables and functions
- Use PascalCase for types, interfaces, and classes
- Use descriptive names that convey intent

### Documentation

- Add JSDoc comments to public APIs
- Update the README if adding new features
- Run `npm run docs` to generate API documentation

## Pull Request Process

1. **Fork and clone** the repository
2. **Create a branch** for your changes: `git checkout -b feature/your-feature`
3. **Make your changes** and ensure all tests pass
4. **Commit your changes** with clear, descriptive commit messages
5. **Push to your fork** and open a Pull Request

### Before Submitting

- [ ] Run `npm run typecheck` to ensure no type errors
- [ ] Run `npm test` to ensure all tests pass
- [ ] Add tests for new functionality
- [ ] Update documentation if needed

### PR Guidelines

- Keep PRs focused - one feature or fix per PR
- Write clear PR descriptions explaining the change
- Reference related issues using `Fixes #123` or `Closes #123`
- Be responsive to review feedback

## Issue Reporting

### Bug Reports

When reporting a bug, please include:

- A clear description of the issue
- Steps to reproduce the problem
- Expected behavior vs actual behavior
- Node.js version and operating system
- Relevant code snippets or error messages

### Feature Requests

For feature requests, please include:

- A clear description of the proposed feature
- Use cases explaining why this feature would be useful
- Any relevant examples or references

## Getting Help

- Check existing [issues](https://github.com/dot-do/deltalake/issues) for similar questions
- Open a new issue if you need help or have questions

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
