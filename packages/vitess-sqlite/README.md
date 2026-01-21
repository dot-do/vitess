# @dotdo/vitess-sqlite

SQLite storage engine for Vitess.do using [Turso/libSQL](https://turso.tech/).

Supports local SQLite files, in-memory databases, and Turso cloud with optional **PostgreSQL dialect translation**.

## Installation

```bash
npm install @dotdo/vitess-sqlite
```

## Quick Start

```typescript
import { TursoAdapter } from '@dotdo/vitess-sqlite';

// Create adapter
const adapter = new TursoAdapter({
  url: ':memory:', // or 'file:mydb.db' or 'libsql://...'
});

// Connect
await adapter.connect();

// Execute queries
const result = await adapter.query<User>(
  'SELECT * FROM users WHERE id = ?',
  [userId]
);

// Close when done
await adapter.close();
```

## Configuration

```typescript
interface TursoAdapterConfig {
  /** Database URL - ':memory:', 'file:path', or 'libsql://...' */
  url: string;
  /** Auth token for remote Turso databases */
  authToken?: string;
  /** Sync URL for embedded replicas */
  syncUrl?: string;
  /** SQL dialect mode - 'sqlite' (default) or 'postgres' (auto-translate) */
  dialect?: 'sqlite' | 'postgres';
}
```

### Configuration Examples

```typescript
// In-memory database
const adapter = new TursoAdapter({ url: ':memory:' });

// Local file
const adapter = new TursoAdapter({ url: 'file:./data/mydb.db' });

// Turso cloud
const adapter = new TursoAdapter({
  url: 'libsql://my-database.turso.io',
  authToken: process.env.TURSO_AUTH_TOKEN,
});

// Embedded replica (edge sync)
const adapter = new TursoAdapter({
  url: 'file:./local-replica.db',
  syncUrl: 'libsql://my-database.turso.io',
  authToken: process.env.TURSO_AUTH_TOKEN,
});

// PostgreSQL dialect mode (auto-translates syntax)
const adapter = new TursoAdapter({
  url: ':memory:',
  dialect: 'postgres',
});
```

## PostgreSQL Dialect Translation

When `dialect: 'postgres'` is enabled, the adapter automatically translates:

### Parameter Placeholders

```sql
-- PostgreSQL style (input)
SELECT * FROM users WHERE id = $1 AND status = $2

-- SQLite style (translated)
SELECT * FROM users WHERE id = ? AND status = ?
```

### Data Types

| PostgreSQL | SQLite |
|------------|--------|
| `SERIAL PRIMARY KEY` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
| `VARCHAR(n)`, `CHAR(n)` | `TEXT` |
| `BOOLEAN` | `INTEGER` |
| `TIMESTAMP`, `TIMESTAMPTZ` | `TEXT` |
| `UUID` | `TEXT` |
| `JSONB`, `JSON` | `TEXT` |
| `BYTEA` | `BLOB` |
| `DOUBLE PRECISION` | `REAL` |
| `BIGINT`, `SMALLINT` | `INTEGER` |

### Functions and Operators

| PostgreSQL | SQLite |
|------------|--------|
| `NOW()` | `datetime('now')` |
| `TRUE` / `FALSE` | `1` / `0` |
| `ILIKE` | `LIKE` |
| `::type` cast | `CAST(... AS type)` |
| `gen_random_uuid()` | UUID generation expression |

### Example with Postgres Dialect

```typescript
const adapter = new TursoAdapter({
  url: ':memory:',
  dialect: 'postgres',
});

await adapter.connect();

// PostgreSQL-style SQL works transparently
await adapter.execute(`
  CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
  )
`);

await adapter.execute(
  'INSERT INTO users (name) VALUES ($1)',
  ['Alice']
);

const result = await adapter.query(
  'SELECT * FROM users WHERE active = TRUE'
);
```

## API Reference

### Connection

#### `connect(): Promise<TursoAdapter>`

Connects to the database. Returns the adapter for chaining.

```typescript
const adapter = await new TursoAdapter({ url: ':memory:' }).connect();
```

#### `close(): Promise<void>`

Closes the connection and rolls back active transactions.

#### `isReady(): boolean`

Returns true if connected.

#### `getConnectionInfo(): ConnectionInfo`

Returns connection information (safe to expose, excludes auth token).

### Query Operations

#### `query<T>(sql, params?, options?): Promise<QueryResult<T>>`

Executes a SELECT query.

```typescript
interface QueryOptions {
  txId?: string;          // Execute within transaction
  dialect?: 'sqlite' | 'postgres';  // Override dialect for this query
}

const result = await adapter.query<User>(
  'SELECT * FROM users WHERE status = ?',
  ['active'],
  { dialect: 'sqlite' }
);
```

#### `execute(sql, params?, options?): Promise<ExecuteResult>`

Executes INSERT, UPDATE, DELETE, or DDL statements.

```typescript
const result = await adapter.execute(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Alice', 'alice@example.com']
);
console.log(`Affected: ${result.affected}, Last ID: ${result.lastInsertId}`);
```

#### `batch(statements, options?): Promise<BatchResult>`

Executes multiple statements in a single round-trip.

```typescript
const result = await adapter.batch([
  { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Alice'] },
  { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Bob'] },
]);
```

### Transactions

#### `begin(options?): Promise<string>`

Begins a transaction and returns the transaction ID.

```typescript
interface BeginOptions {
  readOnly?: boolean;
  timeout?: number;
  mode?: 'deferred' | 'immediate' | 'exclusive';
}

const txId = await adapter.begin({ mode: 'immediate' });

await adapter.execute('UPDATE accounts SET balance = balance - ?', [100], { txId });
await adapter.execute('UPDATE accounts SET balance = balance + ?', [100], { txId });

await adapter.commit(txId);
```

#### `commit(txId: string): Promise<void>`

Commits a transaction.

#### `rollback(txId: string): Promise<void>`

Rolls back a transaction.

#### `transaction<T>(callback, options?): Promise<T>`

Executes a callback within a managed transaction.

```typescript
const result = await adapter.transaction(async (tx) => {
  await tx.execute('UPDATE inventory SET stock = stock - 1 WHERE id = ?', [itemId]);
  await tx.execute('INSERT INTO orders (item_id, user_id) VALUES (?, ?)', [itemId, userId]);
  return { orderId: result.lastInsertId };
}, { dialect: 'sqlite' });
```

### Savepoints

```typescript
const txId = await adapter.begin();

await adapter.savepoint('sp1', { txId });
await adapter.execute('INSERT INTO log (msg) VALUES (?)', ['first'], { txId });

await adapter.savepoint('sp2', { txId });
await adapter.execute('INSERT INTO log (msg) VALUES (?)', ['second'], { txId });

// Rollback to sp2, keeping sp1 changes
await adapter.rollbackToSavepoint('sp2', { txId });

await adapter.commit(txId);
```

## Error Handling

```typescript
import {
  TursoError,
  ConnectionError,
  QueryError,
  SyntaxError,
  ConstraintError,
  TransactionError,
} from '@dotdo/vitess-sqlite';

try {
  await adapter.execute('INSERT INTO users (email) VALUES (?)', ['duplicate@example.com']);
} catch (error) {
  if (error instanceof ConstraintError) {
    console.error(`Constraint type: ${error.constraintType}`);
    // 'UNIQUE' | 'NOT_NULL' | 'CHECK' | 'FOREIGN_KEY' | 'PRIMARY_KEY'
  } else if (error instanceof SyntaxError) {
    console.error(`SQL error at position: ${error.position}`);
  } else if (error instanceof TransactionError) {
    console.error(`Transaction ${error.txId} error: ${error.code}`);
  } else if (error instanceof ConnectionError) {
    console.error(`Connection to ${error.url} failed`);
  } else if (error instanceof TursoError) {
    console.error(`Error code: ${error.code}`);
  }
}
```

### Error Types

| Class | Code | Description |
|-------|------|-------------|
| `ConnectionError` | `CONNECTION_FAILED` | Database connection failed |
| `QueryError` | `QUERY_ERROR` | General query error |
| `SyntaxError` | `SYNTAX_ERROR` | SQL syntax error |
| `ConstraintError` | `CONSTRAINT_VIOLATION` | Constraint violation |
| `TransactionError` | `TRANSACTION_NOT_FOUND` | Transaction not found |
| `TransactionError` | `TRANSACTION_EXPIRED` | Transaction timed out |
| `TransactionError` | `READ_ONLY_TRANSACTION` | Write in read-only tx |

## Events

```typescript
adapter.on('ready', () => {
  console.log('Connected to database');
});

adapter.on('close', () => {
  console.log('Connection closed');
});

adapter.on('error', (error) => {
  console.error('Database error:', error);
});

adapter.on('transaction:begin', (txId) => {
  console.log(`Transaction ${txId} started`);
});

adapter.on('transaction:commit', (txId) => {
  console.log(`Transaction ${txId} committed`);
});

adapter.on('transaction:rollback', (txId) => {
  console.log(`Transaction ${txId} rolled back`);
});
```

## Translation Functions

For advanced use cases, translation functions are exported:

```typescript
import {
  translateParams,
  translatePostgresToSQLite,
} from '@dotdo/vitess-sqlite';

// Translate $n params to ?
const { sql, params } = translateParams(
  'SELECT * FROM t WHERE a = $2 AND b = $1',
  ['first', 'second']
);
// sql: 'SELECT * FROM t WHERE a = ? AND b = ?'
// params: ['second', 'first']

// Translate PostgreSQL syntax
const sqliteSql = translatePostgresToSQLite(
  'CREATE TABLE t (id SERIAL PRIMARY KEY, active BOOLEAN DEFAULT TRUE)'
);
// 'CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, active INTEGER DEFAULT 1)'
```

## Integration with Vitess.do

```typescript
import { VTTablet, TursoEngine } from '@dotdo/vitess/server';
import { TursoAdapter } from '@dotdo/vitess-sqlite';

const adapter = new TursoAdapter({
  url: 'file:./data/shard-0.db',
  dialect: 'postgres',  // Accept Postgres SQL from clients
});
await adapter.connect();

const tablet = new VTTablet({
  shard: '-80',
  keyspace: 'main',
  engine: new TursoEngine(adapter),
});
```

## Related Packages

| Package | Description |
|---------|-------------|
| `@dotdo/vitess` | Main SDK (client + server) |
| `@dotdo/vitess-rpc` | RPC protocol types |
| `@dotdo/vitess-postgres` | PostgreSQL storage engine |

## License

MIT
