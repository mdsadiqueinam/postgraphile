# PostGraphile (Rust)

A Rust implementation inspired by the popular [NodeJS PostGraphile](https://www.postgraphile.org/) project. Automatically generate a GraphQL API from your PostgreSQL database schema.

## Overview

This project automatically introspects your PostgreSQL database and generates a GraphQL API, eliminating boilerplate and keeping your API schema in sync with your database.

## Features

- 🗄️ Automatic schema generation from PostgreSQL
- 🚀 Built with async-graphql for high-performance GraphQL
- 🔗 Built-in row extensions and entity generation
- ⚡ Powered by async Rust with Tokio

## Getting Started

```bash
cargo build
cargo run
```

## Project Structure

- `src/entity_generator.rs` - Generates GraphQL entities from database schema
- `src/schema.rs` - Schema generation and introspection
- `src/extensions/` - Extensions for GraphQL types
- `src/utils/` - Utility functions for inflection and database operations

## Roadmap

- [ ] Complete PostgreSQL schema introspection
- [ ] Generate full CRUD operations
- [ ] Support for computed fields
- [ ] Permission and authentication layer
- [ ] Plugin system for custom extensions
- [ ] CLI tool for easy setup

## License

MIT

## Inspired By

[PostGraphile](https://www.postgraphile.org/) - Instant GraphQL API for PostgreSQL
