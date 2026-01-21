# @dotdo/vitess-postgres

PostgreSQL storage engine for Vitess.do, powered by [PGlite](https://github.com/electric-sql/pglite).

Provides **full PostgreSQL compatibility** via WebAssembly, running entirely in-process without a separate database server. Ideal for applications requiring advanced SQL features, JSONB, or strict type fidelity.

## Installation

```bash
npm install @dotdo/vitess-postgres
```

## Quick Start

```typescript
import { PGliteAdapter } from '@dotdo/vitess-postgres';

// Create adapter (in-memory by default)
const adapter = new PGliteAdapter();

// Initialize
await adapter.init();

// Execute queries
const result = await adapter.query<User>(
  'SELECT * FROM users WHERE id = $1',
  [userId]
);

// Execute writes
const execResult = await adapter.execute(
  'INSERT INTO users (name, email) VALUES ($1, $2)',
  ['Alice', 'alice@example.com']
);
console.log(`Inserted, last ID: ${execResult.lastInsertId}`);

// Close when done
await adapter.close();
```

## Configuration

```typescript
interface PGliteAdapterOptions {
  /** Data directory path (optional, defaults to in-memory) */
  dataDir?: string;
  /** Enable debug logging */
  debug?: boolean;
  /** Initial schema to execute on init */
  initSchema?: string;
}
```

### Examples

```typescript
// In-memory database (default)
const adapter = new PGliteAdapter();

// Persistent storage
const adapter = new PGliteAdapter({
  dataDir: '/data/mydb',
});

// With initial schema
const adapter = new PGliteAdapter({
  initSchema: `
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `,
});
```

## API Reference

### PGliteAdapter

#### `init(): Promise<void>`

Initializes the PGlite instance. Must be called before any queries.

```typescript
const adapter = new PGliteAdapter();
await adapter.init();
```

#### `close(): Promise<void>`

Closes the database connection and rolls back any active transactions.

#### `ready: boolean`

Returns true if the adapter has been initialized.

#### `closed: boolean`

Returns true if the adapter has been closed.

#### `waitReady: Promise<void>`

Promise that resolves when the adapter is ready.

```typescript
// Can be used to wait for initialization
await adapter.waitReady;
```

### Query Operations

#### `query<T>(sql: string, params?: unknown[]): Promise<QueryResult<T>>`

Executes a SELECT query.

```typescript
interface QueryResult<T> {
  rows: T[];
  rowCount: number;
  fields?: Field[];
  duration?: number;
}

const result = await adapter.query<User>(
  'SELECT * FROM users WHERE status = $1 ORDER BY created_at DESC LIMIT $2',
  ['active', 10]
);

for (const user of result.rows) {
  console.log(user.name);
}
```

#### `execute(sql: string, params?: unknown[]): Promise<ExecuteResult>`

Executes INSERT, UPDATE, or DELETE statements.

```typescript
interface ExecuteResult {
  affected: number;
  lastInsertId?: number;
}

// Insert with returning last ID
const result = await adapter.execute(
  'INSERT INTO users (name) VALUES ($1)',
  ['Alice']
);
console.log(`Inserted row with ID: ${result.lastInsertId}`);

// Update
const updateResult = await adapter.execute(
  'UPDATE users SET status = $1 WHERE last_login < $2',
  ['inactive', '2024-01-01']
);
console.log(`Updated ${updateResult.affected} rows`);
```

### Transactions

#### `begin(options?): Promise<PGliteTransaction>`

Begins a new transaction.

```typescript
const tx = await adapter.begin();

try {
  await tx.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, fromId]);
  await tx.execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, toId]);
  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}
```

#### `transaction<T>(callback, options?): Promise<T>`

Executes a callback within a managed transaction with automatic commit/rollback.

```typescript
const result = await adapter.transaction(async (tx) => {
  const balance = await tx.query<{ balance: number }>(
    'SELECT balance FROM accounts WHERE id = $1',
    [accountId]
  );

  if (balance.rows[0].balance < amount) {
    throw new Error('Insufficient funds');
  }

  await tx.execute(
    'UPDATE accounts SET balance = balance - $1 WHERE id = $2',
    [amount, accountId]
  );

  return { success: true };
});
```

### Transaction Options

```typescript
interface TransactionOptions {
  /** Isolation level */
  isolation?: 'read_uncommitted' | 'read_committed' | 'repeatable_read' | 'serializable';
  /** Read-only transaction */
  readOnly?: boolean;
  /** Timeout in milliseconds */
  timeout?: number;
}

// Serializable transaction
const tx = await adapter.begin({ isolation: 'serializable' });

// Read-only transaction
const tx = await adapter.begin({ readOnly: true });

// With timeout
const tx = await adapter.begin({ timeout: 5000 });
```

### PGliteTransaction Interface

```typescript
interface PGliteTransaction {
  readonly id: string;
  readonly active: boolean;
  query<T>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
  execute(sql: string, params?: unknown[]): Promise<ExecuteResult>;
  commit(): Promise<void>;
  rollback(): Promise<void>;
}
```

## Error Handling

```typescript
import { PGliteAdapterError, PGliteErrorCode } from '@dotdo/vitess-postgres';

try {
  await adapter.execute(
    'INSERT INTO users (email) VALUES ($1)',
    ['duplicate@example.com']
  );
} catch (error) {
  if (error instanceof PGliteAdapterError) {
    switch (error.code) {
      case PGliteErrorCode.CONSTRAINT_VIOLATION:
        console.error('Constraint violation:', error.message);
        break;
      case PGliteErrorCode.SYNTAX_ERROR:
        console.error('SQL syntax error:', error.message);
        break;
      case PGliteErrorCode.TYPE_ERROR:
        console.error('Type error:', error.message);
        break;
      case PGliteErrorCode.NOT_READY:
        console.error('Adapter not initialized');
        break;
      case PGliteErrorCode.ALREADY_CLOSED:
        console.error('Adapter already closed');
        break;
      case PGliteErrorCode.TRANSACTION_ERROR:
        console.error('Transaction error:', error.message);
        break;
    }

    // SQL state code (if available)
    if (error.sqlState) {
      console.error('SQL State:', error.sqlState);
    }
  }
}
```

### Error Codes

| Code | Description |
|------|-------------|
| `CONNECTION_ERROR` | Failed to initialize PGlite |
| `QUERY_ERROR` | General query execution error |
| `TRANSACTION_ERROR` | Transaction-related error |
| `TYPE_ERROR` | Data type mismatch |
| `CONSTRAINT_VIOLATION` | Unique, foreign key, or other constraint violated |
| `SYNTAX_ERROR` | SQL syntax error |
| `NOT_READY` | Adapter not initialized |
| `ALREADY_CLOSED` | Adapter has been closed |

## PostgreSQL Type Support

PGlite supports standard PostgreSQL types including:

| PostgreSQL Type | JavaScript Type |
|-----------------|-----------------|
| `bool` | `boolean` |
| `int2`, `int4`, `int8` | `number` / `bigint` |
| `float4`, `float8` | `number` |
| `numeric` | `string` |
| `text`, `varchar`, `char` | `string` |
| `date`, `time`, `timestamp`, `timestamptz` | `Date` or `string` |
| `json`, `jsonb` | `object` |
| `uuid` | `string` |
| `bytea` | `Uint8Array` |
| Arrays | `Array<T>` |

## Integration with Vitess.do

The adapter implements the Vitess storage engine interface:

```typescript
import { VTTablet, PGliteEngine } from '@dotdo/vitess/server';
import { PGliteAdapter } from '@dotdo/vitess-postgres';

// Create and initialize adapter
const adapter = new PGliteAdapter({ dataDir: '/data/shard-0' });
await adapter.init();

// Use with VTTablet Durable Object
const tablet = new VTTablet({
  shard: '-80',
  keyspace: 'main',
  engine: new PGliteEngine(adapter),
});
```

## When to Use PGlite vs Turso

| Factor | PGlite (PostgreSQL) | Turso (SQLite) |
|--------|---------------------|----------------|
| SQL compatibility | Full PostgreSQL | SQLite + translation |
| Cold start | ~200ms | ~50ms |
| Memory usage | Higher | Lower |
| JSONB support | Native | JSON as text |
| Best for | Complex queries, strict types | High scale, edge workloads |

## Documentation

- [Getting Started Guide](../../docs/getting-started.md)
- [Architecture Overview](../../docs/architecture.md)
- [API Reference](../../docs/api.md)

## Related Packages

| Package | Description |
|---------|-------------|
| [@dotdo/vitess](../vitess/README.md) | Main SDK (client + server) |
| [@dotdo/vitess-rpc](../vitess-rpc/README.md) | RPC protocol types |
| [@dotdo/vitess-sqlite](../vitess-sqlite/README.md) | SQLite storage engine |

## License

MIT
