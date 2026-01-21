# Vitess.do API Reference

Complete API documentation for the vitess.do distributed sharding platform.

## Table of Contents

- [Client SDK](#client-sdk)
  - [createClient](#createclient)
  - [VitessClient](#vitessclient)
  - [VitessConfig](#vitessconfig)
- [Query Methods](#query-methods)
  - [query](#query)
  - [execute](#execute)
  - [batch](#batch)
  - [transaction](#transaction)
- [Cluster Management](#cluster-management)
  - [status](#status)
  - [vschema](#vschema)
- [Types](#types)
  - [QueryResult](#queryresult)
  - [ExecuteResult](#executeresult)
  - [BatchResult](#batchresult)
  - [Transaction](#transaction-interface)
  - [ClusterStatus](#clusterstatus)
  - [VSchema](#vschema-type)
- [Errors](#errors)
  - [VitessError](#vitesserror)
  - [Error Codes](#error-codes)
- [Server Components](#server-components)
  - [VTGate](#vtgate)
  - [VTTablet](#vttablet)
- [Storage Engines](#storage-engines)
  - [PGliteAdapter](#pgliteadapter)
  - [TursoAdapter](#tursoadapter)

---

## Client SDK

### createClient

Factory function to create a new Vitess client instance.

```typescript
import { createClient } from '@dotdo/vitess';

const client = createClient({
  endpoint: 'https://my-app.vitess.do',
  keyspace: 'main',
});
```

**Signature:**
```typescript
function createClient(config: VitessConfig): VitessClient
```

**Parameters:**
- `config` - Client configuration options (see [VitessConfig](#vitessconfig))

**Returns:** A new `VitessClient` instance

---

### VitessClient

The main client class for interacting with Vitess.do clusters.

```typescript
const client = new VitessClient({
  endpoint: 'https://my-app.vitess.do',
  keyspace: 'main',
});
```

#### Constructor

```typescript
constructor(config: VitessConfig)
```

#### Methods

| Method | Description |
|--------|-------------|
| `connect()` | Establish connection to VTGate |
| `disconnect()` | Close the connection |
| `isConnected()` | Check connection status |
| `query()` | Execute a SELECT query |
| `execute()` | Execute a write statement |
| `batch()` | Execute multiple statements |
| `transaction()` | Execute statements in a transaction |
| `status()` | Get cluster status |
| `vschema()` | Get VSchema configuration |

---

### VitessConfig

Configuration options for the Vitess client.

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

**Example:**
```typescript
const config: VitessConfig = {
  endpoint: 'https://my-app.vitess.do',
  keyspace: 'main',
  token: process.env.VITESS_TOKEN,
  timeout: 60000,
  retry: {
    maxAttempts: 5,
    backoffMs: 200,
  },
};
```

---

## Query Methods

### query

Execute a read query (SELECT) and return typed results.

```typescript
async query<T extends Row = Row>(
  sql: string,
  params?: unknown[]
): Promise<QueryResult<T>>
```

**Parameters:**
- `sql` - SQL query string with `$1`, `$2`, etc. parameter placeholders
- `params` - Array of parameter values (optional)

**Returns:** `QueryResult<T>` containing the rows and metadata

**Example:**
```typescript
interface User {
  id: number;
  name: string;
  email: string;
}

// Simple query
const result = await client.query<User>('SELECT * FROM users');
console.log(result.rows); // User[]

// Parameterized query
const result = await client.query<User>(
  'SELECT * FROM users WHERE tenant_id = $1 AND active = $2',
  [tenantId, true]
);

// With field metadata
console.log(result.fields); // [{ name: 'id', type: 'int4' }, ...]
console.log(result.rowCount); // number of rows returned
console.log(result.duration); // execution time in ms
```

**Notes:**
- Works identically with both PostgreSQL and SQLite backends
- Parameter placeholders use PostgreSQL syntax (`$1`, `$2`, etc.)
- SQLite backend automatically translates placeholders

---

### execute

Execute a write statement (INSERT, UPDATE, DELETE).

```typescript
async execute(
  sql: string,
  params?: unknown[]
): Promise<ExecuteResult>
```

**Parameters:**
- `sql` - SQL statement with parameter placeholders
- `params` - Array of parameter values (optional)

**Returns:** `ExecuteResult` with affected row count and optional last insert ID

**Example:**
```typescript
// INSERT
const insertResult = await client.execute(
  'INSERT INTO users (name, email) VALUES ($1, $2)',
  ['Alice', 'alice@example.com']
);
console.log(insertResult.affected); // 1
console.log(insertResult.lastInsertId); // auto-generated ID

// UPDATE
const updateResult = await client.execute(
  'UPDATE users SET name = $1 WHERE id = $2',
  ['Bob', 123]
);
console.log(updateResult.affected); // number of updated rows

// DELETE
const deleteResult = await client.execute(
  'DELETE FROM users WHERE inactive_since < $1',
  [thirtyDaysAgo]
);
console.log(deleteResult.affected); // number of deleted rows
```

---

### batch

Execute multiple statements in a single request.

```typescript
async batch(
  statements: Array<{ sql: string; params?: unknown[] }>
): Promise<BatchResult>
```

**Parameters:**
- `statements` - Array of statement objects with `sql` and optional `params`

**Returns:** `BatchResult` with results for each statement

**Example:**
```typescript
const result = await client.batch([
  { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Alice'] },
  { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Bob'] },
  { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Charlie'] },
]);

console.log(result.success); // true if all succeeded
console.log(result.results); // QueryResult[] for each statement

// Handle partial failure
if (!result.success) {
  console.log(`Failed at statement ${result.failedAt}: ${result.error}`);
}
```

**Notes:**
- Statements may be executed atomically depending on the backend
- Use `transaction()` for guaranteed atomicity

---

### transaction

Execute statements within a transaction with automatic commit/rollback.

```typescript
async transaction<T>(
  fn: (tx: Transaction) => Promise<T>,
  options?: TransactionOptions
): Promise<T>
```

**Parameters:**
- `fn` - Async function receiving a `Transaction` object
- `options` - Optional transaction configuration

**Returns:** The return value of `fn` if successful

**Example:**
```typescript
// Simple transaction
await client.transaction(async (tx) => {
  await tx.execute(
    'UPDATE accounts SET balance = balance - $1 WHERE id = $2',
    [100, fromAccountId]
  );
  await tx.execute(
    'UPDATE accounts SET balance = balance + $1 WHERE id = $2',
    [100, toAccountId]
  );
});

// Transaction with return value
const total = await client.transaction(async (tx) => {
  const result = await tx.query<{ balance: number }>(
    'SELECT SUM(balance) as balance FROM accounts WHERE user_id = $1',
    [userId]
  );
  return result.rows[0].balance;
});

// Transaction with options
await client.transaction(
  async (tx) => {
    // ... operations
  },
  {
    isolation: 'serializable',
    readOnly: true,
    timeout: 5000,
  }
);
```

**Transaction auto-commits on success, auto-rollbacks on error:**
```typescript
try {
  await client.transaction(async (tx) => {
    await tx.execute('INSERT INTO orders (user_id) VALUES ($1)', [userId]);
    // If this throws, the INSERT is automatically rolled back
    await tx.execute('UPDATE inventory SET qty = qty - $1 WHERE item_id = $2', [qty, itemId]);
  });
} catch (error) {
  // Transaction was rolled back, handle error
}
```

---

## Cluster Management

### status

Get current cluster health and status information.

```typescript
async status(): Promise<ClusterStatus>
```

**Returns:** `ClusterStatus` object with cluster-wide metrics

**Example:**
```typescript
const status = await client.status();

console.log(status.keyspace);      // 'main'
console.log(status.shardCount);    // 4
console.log(status.engine);        // 'postgres' or 'sqlite'
console.log(status.totalQueries);  // 12345
console.log(status.totalErrors);   // 2

// Check individual shard health
for (const shard of status.shards) {
  console.log(`Shard ${shard.id}: ${shard.healthy ? 'healthy' : 'unhealthy'}`);
  console.log(`  Queries: ${shard.queryCount}, Errors: ${shard.errorCount}`);
  if (shard.latency) {
    console.log(`  p50: ${shard.latency.p50}ms, p99: ${shard.latency.p99}ms`);
  }
}
```

---

### vschema

Get the VSchema (sharding configuration) for the current keyspace.

```typescript
async vschema(): Promise<VSchema>
```

**Returns:** `VSchema` object with table and vindex definitions

**Example:**
```typescript
const schema = await client.vschema();

console.log(schema.keyspace);  // 'main'
console.log(schema.sharded);   // true

// Inspect tables
for (const [tableName, tableDef] of Object.entries(schema.tables)) {
  console.log(`Table: ${tableName}`);
  console.log(`  Vindex: ${tableDef.vindex.type}`);
  console.log(`  Sharding columns: ${tableDef.vindex.columns.join(', ')}`);
}

// Inspect vindexes
for (const [vindexName, vindexDef] of Object.entries(schema.vindexes)) {
  console.log(`Vindex: ${vindexName} (${vindexDef.type})`);
}
```

---

## Types

### QueryResult

Result of a SELECT query.

```typescript
interface QueryResult<T extends Row = Row> {
  /** Array of result rows */
  rows: T[];

  /** Number of rows returned */
  rowCount: number;

  /** Column metadata */
  fields?: Field[];

  /** Query execution time in milliseconds */
  duration?: number;
}
```

### Row

Generic row type - a record with string keys and unknown values.

```typescript
type Row = Record<string, unknown>;
```

### Field

Column metadata from query results.

```typescript
interface Field {
  /** Column name */
  name: string;

  /** Normalized type name (e.g., 'int4', 'text', 'bool') */
  type: string;

  /** Original database-specific type (OID for Postgres, SQLite type) */
  nativeType?: string | number;
}
```

---

### ExecuteResult

Result of a write operation (INSERT, UPDATE, DELETE).

```typescript
interface ExecuteResult {
  /** Number of rows affected */
  affected: number;

  /** Last inserted row ID (for INSERT with auto-increment) */
  lastInsertId?: string | number;
}
```

---

### BatchResult

Result of a batch operation.

```typescript
interface BatchResult {
  /** Results for each statement */
  results: QueryResult[];

  /** Whether all statements succeeded */
  success: boolean;

  /** Index of the failed statement (if any) */
  failedAt?: number;

  /** Error message (if failed) */
  error?: string;
}
```

---

### Transaction Interface

Handle for executing operations within a transaction.

```typescript
interface Transaction {
  /** Transaction ID */
  readonly id: string;

  /** Shards involved in this transaction */
  readonly shards: string[];

  /** Execute a query within the transaction */
  query<T extends Row = Row>(
    sql: string,
    params?: unknown[]
  ): Promise<QueryResult<T>>;

  /** Execute a write statement within the transaction */
  execute(sql: string, params?: unknown[]): Promise<ExecuteResult>;

  /** Commit the transaction (called automatically on success) */
  commit(): Promise<void>;

  /** Rollback the transaction (called automatically on error) */
  rollback(): Promise<void>;
}
```

### TransactionOptions

Configuration options for transactions.

```typescript
interface TransactionOptions {
  /** Isolation level */
  isolation?: IsolationLevel;

  /** Read-only transaction (performance optimization) */
  readOnly?: boolean;

  /** Timeout in milliseconds */
  timeout?: number;
}

type IsolationLevel =
  | 'read_uncommitted'
  | 'read_committed'
  | 'repeatable_read'
  | 'serializable';
```

---

### ClusterStatus

Cluster health and metrics.

```typescript
interface ClusterStatus {
  /** Keyspace name */
  keyspace: string;

  /** Number of shards */
  shardCount: number;

  /** Storage engine type */
  engine: StorageEngineType;

  /** Per-shard health information */
  shards: ShardHealth[];

  /** Total queries across all shards */
  totalQueries: number;

  /** Total errors across all shards */
  totalErrors: number;
}
```

### ShardHealth

Health status for a single shard.

```typescript
interface ShardHealth {
  /** Shard identifier (e.g., '-80', '80-ff') */
  id: string;

  /** Whether the shard is healthy */
  healthy: boolean;

  /** Storage engine type */
  engine: StorageEngineType;

  /** Total queries on this shard */
  queryCount: number;

  /** Total errors on this shard */
  errorCount: number;

  /** Last query timestamp */
  lastQuery: number;

  /** Latency percentiles in milliseconds */
  latency?: {
    p50: number;
    p95: number;
    p99: number;
  };
}
```

---

### VSchema Type

Sharding configuration schema.

```typescript
interface VSchema {
  /** Keyspace name */
  keyspace: string;

  /** Whether the keyspace is sharded */
  sharded: boolean;

  /** Table definitions */
  tables: Record<string, TableDef>;

  /** Vindex definitions */
  vindexes: Record<string, VindexDef>;
}

interface TableDef {
  /** Primary vindex for this table */
  vindex: VindexDef;

  /** Auto-increment configuration */
  autoIncrement?: {
    column: string;
    sequence: string;
  };
}

interface VindexDef {
  /** Vindex type */
  type: VindexType;

  /** Column(s) used for sharding */
  columns: string[];

  /** Lookup table (for lookup vindexes) */
  lookupTable?: string;
}

type VindexType =
  | 'hash'            // Hash-based sharding
  | 'consistent_hash' // Consistent hash ring
  | 'range'           // Range-based sharding
  | 'lookup'          // Lookup table for routing
  | 'null';           // No sharding (single shard)
```

---

## Errors

### VitessError

Custom error class for Vitess-specific errors.

```typescript
class VitessError extends Error {
  /** Error code */
  readonly code: string;

  /** Shard that caused the error (if applicable) */
  readonly shard?: string;

  constructor(code: string, message: string, shard?: string);
}
```

**Example:**
```typescript
try {
  await client.query('SELECT * FROM nonexistent');
} catch (error) {
  if (error instanceof VitessError) {
    console.log(`Error code: ${error.code}`);
    console.log(`Message: ${error.message}`);
    if (error.shard) {
      console.log(`Failed on shard: ${error.shard}`);
    }
  }
}
```

---

### Error Codes

Common error codes returned by VitessError.

| Code | Description |
|------|-------------|
| `CONNECTION_ERROR` | Failed to connect to VTGate |
| `QUERY_ERROR` | Query execution failed |
| `TRANSACTION_ERROR` | Transaction operation failed |
| `SYNTAX_ERROR` | SQL syntax error |
| `CONSTRAINT_VIOLATION` | Constraint (unique, FK, etc.) violated |
| `TYPE_ERROR` | Type conversion or validation error |
| `NOT_FOUND` | Table or column not found |
| `TIMEOUT` | Operation timed out |
| `UNAUTHORIZED` | Authentication failed |
| `SHARD_ERROR` | Shard-level error occurred |

**PostgreSQL-specific codes (PGlite):**
| Code | Description |
|------|-------------|
| `PGLITE_CONNECTION_ERROR` | PGlite connection failed |
| `PGLITE_QUERY_ERROR` | PGlite query failed |
| `PGLITE_TRANSACTION_ERROR` | PGlite transaction error |
| `PGLITE_NOT_READY` | PGlite not initialized |
| `PGLITE_ALREADY_CLOSED` | PGlite already closed |

**SQLite-specific codes (Turso):**
| Code | Description |
|------|-------------|
| `CONNECTION_FAILED` | Turso connection failed |
| `QUERY_ERROR` | Turso query failed |
| `SYNTAX_ERROR` | SQL syntax error |
| `CONSTRAINT_VIOLATION` | Constraint violated |
| `TRANSACTION_NOT_FOUND` | Transaction ID not found |
| `TRANSACTION_EXPIRED` | Transaction timed out |
| `READ_ONLY_TRANSACTION` | Write in read-only transaction |

---

## Server Components

### VTGate

Query router responsible for:
- Parsing incoming queries
- Determining target shard(s) using VSchema/Vindexes
- Single-shard routing for point queries
- Scatter-gather for cross-shard queries
- Result aggregation (COUNT, SUM, AVG, MIN, MAX)
- Transaction coordination

```typescript
import { VTGate } from '@dotdo/vitess/server';

const gate = new VTGate({
  vschema: myVSchema,
  shards: new Map([
    ['main', ['-80', '80-']],
  ]),
  tablets: tabletMap,
  defaultKeyspace: 'main',
});

// Route a query
const route = gate.route('SELECT * FROM users WHERE id = $1', [123]);
console.log(route.shards);  // ['-80'] - single shard
console.log(route.scatter); // false

// Plan query execution
const plan = gate.plan('SELECT COUNT(*) FROM users');
console.log(plan.type);     // 'scatter_aggregate'

// Execute query with routing
const result = await gate.execute('SELECT * FROM users WHERE tenant_id = $1', [tenantId]);
```

---

### VTTablet

Shard-level query executor responsible for:
- Executing queries on the local shard
- Transaction management
- Storage engine abstraction
- Connection pooling

```typescript
import { VTTablet, PGliteEngine } from '@dotdo/vitess/server';

const tablet = new VTTablet({
  shard: '-80',
  keyspace: 'main',
  engine: new PGliteEngine(pgliteInstance),
  maxTransactions: 100,
});

// Execute query
const result = await tablet.query('SELECT * FROM users WHERE id = $1', [123]);

// Begin transaction
const tx = await tablet.beginTransaction();
await tx.execute('UPDATE users SET name = $1 WHERE id = $2', ['Alice', 123]);
await tx.commit();

// Switch storage engine (hot swap)
await tablet.switchEngine(newEngine);

// Close
await tablet.close();
```

---

## Storage Engines

### PGliteAdapter

PostgreSQL storage engine using PGlite WASM.

```typescript
import { PGliteAdapter } from '@dotdo/vitess-postgres';

const adapter = new PGliteAdapter({
  dataDir: '/path/to/data',  // optional, defaults to in-memory
  debug: false,
  initSchema: 'CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT)',
});

// Initialize
await adapter.init();

// Query
const result = await adapter.query<User>('SELECT * FROM users WHERE id = $1', [1]);

// Execute
const exec = await adapter.execute('INSERT INTO users (name) VALUES ($1)', ['Alice']);

// Transaction
const tx = await adapter.begin({ isolation: 'serializable' });
await tx.execute('UPDATE users SET name = $1 WHERE id = $2', ['Bob', 1]);
await tx.commit();

// Transaction with callback
await adapter.transaction(async (tx) => {
  await tx.query('SELECT * FROM users FOR UPDATE');
  await tx.execute('UPDATE users SET count = count + 1');
});

// Close
await adapter.close();
```

**Configuration:**
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

---

### TursoAdapter

SQLite storage engine using Turso/libSQL.

```typescript
import { TursoAdapter } from '@dotdo/vitess-sqlite';

const adapter = new TursoAdapter({
  url: 'libsql://my-database.turso.io',
  authToken: process.env.TURSO_AUTH_TOKEN,
  dialect: 'postgres',  // auto-translate Postgres SQL to SQLite
});

// Connect
await adapter.connect();

// Query (uses Postgres syntax, auto-translated)
const result = await adapter.query(
  'SELECT * FROM users WHERE created_at > NOW() - INTERVAL $1 DAY',
  [7]
);

// Execute
await adapter.execute(
  'INSERT INTO users (id, name) VALUES ($1, $2)',
  [uuid(), 'Alice']
);

// Transaction
const txId = await adapter.begin();
await adapter.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, 1], { txId });
await adapter.execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, 2], { txId });
await adapter.commit(txId);

// Close
await adapter.close();
```

**Configuration:**
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

**Dialect Translation:**

When `dialect: 'postgres'` is set, the adapter automatically translates:

| PostgreSQL | SQLite |
|------------|--------|
| `$1`, `$2`, ... | `?`, `?`, ... |
| `SERIAL PRIMARY KEY` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
| `VARCHAR(n)` | `TEXT` |
| `BOOLEAN` | `INTEGER` |
| `TIMESTAMP` | `TEXT` |
| `JSONB` | `TEXT` |
| `NOW()` | `datetime('now')` |
| `TRUE` / `FALSE` | `1` / `0` |
| `ILIKE` | `LIKE` |

---

## RPC Protocol

Vitess.do uses the CapnWeb RPC protocol for client-server communication.

### Message Types

```typescript
enum MessageType {
  // Query operations
  QUERY = 0x01,
  EXECUTE = 0x02,
  BATCH = 0x03,

  // Transaction operations
  BEGIN = 0x10,
  COMMIT = 0x11,
  ROLLBACK = 0x12,

  // Admin operations
  STATUS = 0x20,
  HEALTH = 0x21,
  SCHEMA = 0x22,
  VSCHEMA = 0x23,

  // Shard operations (VTGate -> VTTablet)
  SHARD_QUERY = 0x30,
  SHARD_EXECUTE = 0x31,
  SHARD_BATCH = 0x32,

  // Response types
  RESULT = 0x80,
  ERROR = 0x81,
  ACK = 0x82,
}
```

### Request/Response Factory Functions

```typescript
import {
  createQueryRequest,
  createExecuteRequest,
  createBatchRequest,
  createBeginRequest,
  createCommitRequest,
  createRollbackRequest,
  createStatusRequest,
  createHealthRequest,
  createErrorResponse,
} from '@dotdo/vitess-rpc';

// Create a query request
const request = createQueryRequest(
  'SELECT * FROM users WHERE id = $1',
  [123],
  { keyspace: 'main' }
);

// Create an error response
const error = createErrorResponse(
  request.id,
  'QUERY_ERROR',
  'Table not found',
  { shard: '-80' }
);
```
