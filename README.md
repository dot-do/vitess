# Vitess.do

Vitess-style distributed sharding for Cloudflare Workers and Durable Objects.

Provides **automatic query routing**, **cross-shard transactions**, and **horizontal scaling** with support for both **PostgreSQL (PGlite)** and **SQLite (Turso)** backends.

## Packages

| Package | Description |
|---------|-------------|
| [@dotdo/vitess](./packages/vitess/README.md) | Client SDK + VTGate/VTTablet server runtime |
| [@dotdo/vitess-rpc](./packages/vitess-rpc/README.md) | CapnWeb RPC protocol and unified types |
| [@dotdo/vitess-postgres](./packages/vitess-postgres/README.md) | PostgreSQL storage engine (PGlite) |
| [@dotdo/vitess-sqlite](./packages/vitess-sqlite/README.md) | SQLite storage engine (Turso/libSQL) |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client SDK                               │
│                      @dotdo/vitess                               │
└─────────────────────────┬───────────────────────────────────────┘
                          │ CapnWeb RPC
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    VTGate (Worker)                               │
│              Query routing, aggregation                          │
└─────────────────────────┬───────────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ VTTablet DO #0  │ │ VTTablet DO #1  │ │ VTTablet DO #N  │
│  (Shard 0)      │ │  (Shard 1)      │ │  (Shard N)      │
└────────┬────────┘ └────────┬────────┘ └────────┬────────┘
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  StorageEngine  │ │  StorageEngine  │ │  StorageEngine  │
│  (PGlite or     │ │  (PGlite or     │ │  (PGlite or     │
│   Turso)        │ │   Turso)        │ │   Turso)        │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

## Usage

### Client SDK

```typescript
import { createClient } from '@dotdo/vitess';

const client = createClient({
  endpoint: 'https://my-app.vitess.do',
  keyspace: 'main',
});

await client.connect();

// Query (backend-agnostic - works with Postgres or SQLite)
const users = await client.query<User>(
  'SELECT * FROM users WHERE tenant_id = $1',
  [tenantId]
);

// Transaction
await client.transaction(async (tx) => {
  await tx.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, fromId]);
  await tx.execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, toId]);
});

await client.disconnect();
```

### Server (Cloudflare Workers)

```typescript
import { VTGate, VTTablet } from '@dotdo/vitess/server';
import { PGliteAdapter } from '@dotdo/vitess-postgres';
// Or: import { TursoAdapter } from '@dotdo/vitess-sqlite';

export default VTGate;
export { VTTablet };
```

## Storage Engines

### PostgreSQL (PGlite)

Full PostgreSQL compatibility via WASM. Use `@dotdo/vitess-postgres`.

- Native Postgres SQL syntax
- JSONB support
- Extensions (future)

### SQLite (Turso/libSQL)

Lightweight, fast SQLite via Turso. Use `@dotdo/vitess-sqlite`.

- Edge-sync capable
- Smaller footprint
- Accepts Postgres dialect with auto-translation

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](./docs/getting-started.md) | Installation and first queries |
| [Architecture](./docs/architecture.md) | System overview and component design |
| [API Reference](./docs/api.md) | Complete API documentation |
| [Migration Guide](./docs/migration.md) | Migrating from other solutions |

### Package READMEs

- [@dotdo/vitess](./packages/vitess/README.md) - Client SDK and server runtime
- [@dotdo/vitess-rpc](./packages/vitess-rpc/README.md) - RPC protocol and types
- [@dotdo/vitess-postgres](./packages/vitess-postgres/README.md) - PostgreSQL storage engine
- [@dotdo/vitess-sqlite](./packages/vitess-sqlite/README.md) - SQLite storage engine

## Quick Start

```bash
# Install the SDK and a storage engine
npm install @dotdo/vitess @dotdo/vitess-postgres
```

See [Getting Started](./docs/getting-started.md) for detailed installation and usage instructions.

## Status

**In Development** - See [beads issues](/.beads/) for progress.

## License

MIT
