# @dotdo/vitess-rpc

CapnWeb RPC protocol and unified type definitions for Vitess.do.

This package defines the wire protocol for communication between:
- **VitessClient** and **VTGate** (client-to-server)
- **VTGate** and **VTTablet** Durable Objects (server-to-shard)

## Installation

```bash
npm install @dotdo/vitess-rpc
```

> **Note:** Most users should install `@dotdo/vitess` instead, which re-exports all necessary types. Install this package directly only if you need protocol-level access.

## Overview

The RPC protocol is designed to be:

- **Backend-agnostic**: Works identically with PostgreSQL and SQLite storage engines
- **Type-safe**: Full TypeScript support with type guards and validation
- **Efficient**: Minimal serialization overhead using JSON transport

## Type Exports

### Result Types

```typescript
import type {
  QueryResult,
  ExecuteResult,
  BatchResult,
  Row,
  Field,
} from '@dotdo/vitess-rpc';

// Query result from SELECT operations
interface QueryResult<T extends Row = Row> {
  rows: T[];
  rowCount: number;
  fields?: Field[];
  duration?: number;
}

// Result from INSERT/UPDATE/DELETE
interface ExecuteResult {
  affected: number;
  lastInsertId?: string | number;
}

// Result from batch operations
interface BatchResult {
  results: QueryResult[];
  success: boolean;
  failedAt?: number;
  error?: string;
}

// Generic row type
type Row = Record<string, unknown>;

// Field metadata
interface Field {
  name: string;
  type: string;
  nativeType?: string | number;
}
```

### Transaction Types

```typescript
import type {
  TransactionOptions,
  IsolationLevel,
} from '@dotdo/vitess-rpc';

type IsolationLevel =
  | 'read_uncommitted'
  | 'read_committed'
  | 'repeatable_read'
  | 'serializable';

interface TransactionOptions {
  isolation?: IsolationLevel;
  readOnly?: boolean;
  timeout?: number;
}
```

### VSchema Types

```typescript
import type {
  VSchema,
  TableDef,
  VindexDef,
  VindexType,
  Keyspace,
} from '@dotdo/vitess-rpc';

type VindexType = 'hash' | 'consistent_hash' | 'range' | 'lookup' | 'null';

interface VindexDef {
  type: VindexType;
  columns: string[];
  lookupTable?: string;
}

interface TableDef {
  vindex: VindexDef;
  autoIncrement?: {
    column: string;
    sequence: string;
  };
}

interface VSchema {
  keyspace: string;
  sharded: boolean;
  tables: Record<string, TableDef>;
  vindexes: Record<string, VindexDef>;
}
```

### Cluster Status Types

```typescript
import type {
  ClusterStatus,
  ShardHealth,
  StorageEngineType,
} from '@dotdo/vitess-rpc';

type StorageEngineType = 'postgres' | 'sqlite';

interface ShardHealth {
  id: string;
  healthy: boolean;
  engine: StorageEngineType;
  queryCount: number;
  errorCount: number;
  lastQuery: number;
  latency?: {
    p50: number;
    p95: number;
    p99: number;
  };
}

interface ClusterStatus {
  keyspace: string;
  shardCount: number;
  engine: StorageEngineType;
  shards: ShardHealth[];
  totalQueries: number;
  totalErrors: number;
}
```

## Protocol Messages

### Message Types

```typescript
import { MessageType } from '@dotdo/vitess-rpc';

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

### Request Factory Functions

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
  createVSchemaRequest,
} from '@dotdo/vitess-rpc';

// Create a query request
const request = createQueryRequest(
  'SELECT * FROM users WHERE id = $1',
  [userId],
  { keyspace: 'main' }
);

// Create an execute request
const execRequest = createExecuteRequest(
  'INSERT INTO users (name) VALUES ($1)',
  ['Alice'],
  { keyspace: 'main', txId: 'tx-123' }
);
```

### Response Factory Functions

```typescript
import {
  createQueryResponse,
  createExecuteResponse,
  createErrorResponse,
  createBeginResponse,
  createAckResponse,
} from '@dotdo/vitess-rpc';

// Create query response
const response = createQueryResponse(requestId, {
  rows: [{ id: 1, name: 'Alice' }],
  rowCount: 1,
  fields: [{ name: 'id', type: 'int4' }, { name: 'name', type: 'text' }],
});

// Create error response
const errorResponse = createErrorResponse(
  requestId,
  'CONSTRAINT_VIOLATION',
  'Duplicate key violates unique constraint',
  { shard: 'shard-0', sqlState: '23505' }
);
```

## Type Guards

The package provides comprehensive type guards for runtime validation:

```typescript
import {
  isQueryResult,
  isExecuteResult,
  isVSchema,
  isShardHealth,
  isStorageEngineType,
  isVindexType,
  isIsolationLevel,
  isRequest,
  isResponse,
  isErrorResponse,
} from '@dotdo/vitess-rpc';

// Validate query result
if (isQueryResult(data)) {
  console.log(`Got ${data.rowCount} rows`);
}

// Validate message types
if (isErrorResponse(message)) {
  console.error(`Error: ${message.code} - ${message.message}`);
}
```

## Validation Functions

For more detailed error messages, use validation functions:

```typescript
import {
  validateField,
  validateQueryResult,
  validateExecuteResult,
  validateVSchema,
  validateVindexDef,
} from '@dotdo/vitess-rpc';

const error = validateVSchema(data);
if (error !== null) {
  console.error(`Invalid VSchema: ${error}`);
}
```

## Serialization

```typescript
import {
  serializeMessage,
  deserializeMessage,
  serializeRequest,
  deserializeRequest,
  serializeResponse,
  deserializeResponse,
  safeJsonParse,
  safeJsonStringify,
} from '@dotdo/vitess-rpc';

// Serialize for transport
const json = serializeMessage(request);

// Deserialize from transport
const message = deserializeMessage(json);
```

## Protocol Subpath Export

For advanced use cases, protocol request and response types can be imported directly:

```typescript
import type {
  QueryRequest,
  ExecuteRequest,
  BatchRequest,
  BeginRequest,
  CommitRequest,
  RollbackRequest,
  QueryResponse,
  ExecuteResponse,
  ErrorResponse,
  AckResponse,
} from '@dotdo/vitess-rpc/protocol';
```

## Documentation

- [Architecture Overview](../../docs/architecture.md) - Protocol flow diagrams
- [API Reference](../../docs/api.md) - Full type documentation

## Related Packages

| Package | Description |
|---------|-------------|
| [@dotdo/vitess](../vitess/README.md) | Main SDK (client + server) |
| [@dotdo/vitess-postgres](../vitess-postgres/README.md) | PostgreSQL storage engine |
| [@dotdo/vitess-sqlite](../vitess-sqlite/README.md) | SQLite storage engine |

## License

MIT
