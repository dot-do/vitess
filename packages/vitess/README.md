# @dotdo/vitess

Vitess-style distributed sharding for Cloudflare Workers and Durable Objects.

Provides both a **client SDK** for queries and transactions, and **server components** (VTGate and VTTablet) for deployment. Supports **PostgreSQL (PGlite)** and **SQLite (Turso)** backends through a unified API.

## Installation

```bash
npm install @dotdo/vitess
```

Install one or both storage engine packages:

```bash
# PostgreSQL backend (full Postgres compatibility via WASM)
npm install @dotdo/vitess-postgres

# SQLite backend (lightweight, edge-ready)
npm install @dotdo/vitess-sqlite
```

## Quick Start

### Client Usage

```typescript
import { createClient } from '@dotdo/vitess';

// Create and connect to VTGate
const client = createClient({
  endpoint: 'https://my-app.vitess.do',
  keyspace: 'main',
});
await client.connect();

// Execute queries (identical syntax for Postgres and SQLite backends)
const users = await client.query<User>(
  'SELECT * FROM users WHERE tenant_id = $1',
  [tenantId]
);

// Execute writes
const result = await client.execute(
  'INSERT INTO users (name, email) VALUES ($1, $2)',
  ['Alice', 'alice@example.com']
);
console.log(`Inserted ${result.affected} rows`);

// Batch operations
await client.batch([
  { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Alice'] },
  { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Bob'] },
]);

// Transactions with automatic commit/rollback
await client.transaction(async (tx) => {
  await tx.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, fromId]);
  await tx.execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, toId]);
});

await client.disconnect();
```

### Server Usage (Cloudflare Workers)

```typescript
// src/index.ts
import { VTGate, VTTablet } from '@dotdo/vitess/server';
import { PGliteAdapter } from '@dotdo/vitess-postgres';
// Or: import { TursoAdapter } from '@dotdo/vitess-sqlite';

// Export VTGate as the Worker entry point
export default VTGate;

// Export VTTablet as a Durable Object
export { VTTablet };
```

```toml
# wrangler.toml
name = "my-vitess-app"
main = "src/index.ts"

[[durable_objects.bindings]]
name = "VT_TABLET"
class_name = "VTTablet"

[[migrations]]
tag = "v1"
new_classes = ["VTTablet"]
```

## Configuration

### Client Configuration

```typescript
interface VitessConfig {
  /** VTGate endpoint URL */
  endpoint: string;
  /** Default keyspace */
  keyspace?: string;
  /** Authentication token */
  token?: string;
  /** Request timeout in milliseconds (default: 30000) */
  timeout?: number;
  /** Retry configuration */
  retry?: {
    maxAttempts: number;  // default: 3
    backoffMs: number;    // default: 100
  };
}
```

### Example with Full Configuration

```typescript
const client = createClient({
  endpoint: 'https://my-app.vitess.do',
  keyspace: 'main',
  token: process.env.VITESS_TOKEN,
  timeout: 60000,
  retry: {
    maxAttempts: 5,
    backoffMs: 200,
  },
});
```

## API Reference

### VitessClient

#### `connect(): Promise<void>`

Establishes connection to VTGate.

#### `disconnect(): Promise<void>`

Closes the connection.

#### `isConnected(): boolean`

Returns true if connected.

#### `query<T>(sql: string, params?: unknown[]): Promise<QueryResult<T>>`

Executes a SELECT query and returns results.

```typescript
interface QueryResult<T> {
  rows: T[];
  rowCount: number;
  fields?: Field[];
  duration?: number;
}
```

#### `execute(sql: string, params?: unknown[]): Promise<ExecuteResult>`

Executes INSERT/UPDATE/DELETE statements.

```typescript
interface ExecuteResult {
  affected: number;
  lastInsertId?: string | number;
}
```

#### `batch(statements): Promise<BatchResult>`

Executes multiple statements in a batch.

```typescript
const result = await client.batch([
  { sql: 'INSERT INTO t (a) VALUES ($1)', params: [1] },
  { sql: 'INSERT INTO t (a) VALUES ($1)', params: [2] },
]);
```

#### `transaction<T>(fn, options?): Promise<T>`

Executes a function within a transaction with automatic commit/rollback.

```typescript
interface TransactionOptions {
  isolation?: 'read_uncommitted' | 'read_committed' | 'repeatable_read' | 'serializable';
  readOnly?: boolean;
  timeout?: number;
}
```

#### `status(): Promise<ClusterStatus>`

Returns cluster health status including per-shard metrics.

#### `vschema(): Promise<VSchema>`

Returns the current VSchema configuration.

## Error Handling

```typescript
import { VitessError } from '@dotdo/vitess';

try {
  await client.execute('INSERT INTO users (email) VALUES ($1)', ['duplicate@example.com']);
} catch (error) {
  if (error instanceof VitessError) {
    console.error(`Error code: ${error.code}`);
    console.error(`Message: ${error.message}`);
    console.error(`Shard: ${error.shard}`); // Which shard failed
  }
}
```

## Type Exports

The package re-exports common types from `@dotdo/vitess-rpc`:

```typescript
import type {
  QueryResult,
  ExecuteResult,
  BatchResult,
  Row,
  Field,
  TransactionOptions,
  ClusterStatus,
  ShardHealth,
  VSchema,
  StorageEngineType,
} from '@dotdo/vitess';
```

## Documentation

- [Getting Started Guide](../../docs/getting-started.md)
- [Architecture Overview](../../docs/architecture.md)
- [API Reference](../../docs/api.md)
- [Migration Guide](../../docs/migration.md)

## Related Packages

| Package | Description |
|---------|-------------|
| [@dotdo/vitess-rpc](../vitess-rpc/README.md) | RPC protocol and type definitions |
| [@dotdo/vitess-postgres](../vitess-postgres/README.md) | PostgreSQL storage engine (PGlite) |
| [@dotdo/vitess-sqlite](../vitess-sqlite/README.md) | SQLite storage engine (Turso/libSQL) |

## License

MIT
