# Vitess.do Architecture

Vitess.do brings Vitess-style distributed sharding to Cloudflare Workers and Durable Objects. This document explains the core components, data flow, and how they work together to provide horizontal scaling with automatic query routing.

## Architecture Overview

```
                                    ┌─────────────────────────────────────────────────────┐
                                    │                     Clients                           │
                                    │        (Web Apps, APIs, Mobile, CLI)                 │
                                    └─────────────────────────┬───────────────────────────┘
                                                              │
                                                              │ CapnWeb RPC
                                                              │ (JSON over HTTP)
                                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    VTGate (Worker)                                           │
│                                                                                              │
│   ┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌────────────────┐  │
│   │    Query     │    │      Query       │    │      Shard       │    │    Result      │  │
│   │    Parser    │───▶│     Planner      │───▶│     Router       │───▶│   Aggregator   │  │
│   └──────────────┘    └──────────────────┘    └──────────────────┘    └────────────────┘  │
│                               │                        │                                    │
│                               │ VSchema                │ Vindexes                           │
│                               ▼                        ▼                                    │
│                       ┌──────────────────────────────────────────┐                         │
│                       │           Routing Tables                  │                         │
│                       │   (VSchema + Vindex Configuration)        │                         │
│                       └──────────────────────────────────────────┘                         │
└──────────────────────────────────────────────┬──────────────────────────────────────────────┘
                                               │
               ┌───────────────────────────────┼───────────────────────────────┐
               │                               │                               │
               ▼                               ▼                               ▼
┌──────────────────────────┐   ┌──────────────────────────┐   ┌──────────────────────────┐
│     VTTablet DO          │   │     VTTablet DO          │   │     VTTablet DO          │
│       (Shard -80)        │   │      (Shard 80-c0)       │   │      (Shard c0-)         │
│                          │   │                          │   │                          │
│   ┌──────────────────┐   │   │   ┌──────────────────┐   │   │   ┌──────────────────┐   │
│   │  Query Executor  │   │   │   │  Query Executor  │   │   │   │  Query Executor  │   │
│   └────────┬─────────┘   │   │   └────────┬─────────┘   │   │   └────────┬─────────┘   │
│            │             │   │            │             │   │            │             │
│   ┌────────▼─────────┐   │   │   ┌────────▼─────────┐   │   │   ┌────────▼─────────┐   │
│   │  Storage Engine  │   │   │   │  Storage Engine  │   │   │   │  Storage Engine  │   │
│   │                  │   │   │   │                  │   │   │   │                  │   │
│   │  ┌────────────┐  │   │   │   │  ┌────────────┐  │   │   │   │  ┌────────────┐  │   │
│   │  │  PGlite    │  │   │   │   │  │  PGlite    │  │   │   │   │  │   Turso    │  │   │
│   │  │    or      │  │   │   │   │  │    or      │  │   │   │   │  │    or      │  │   │
│   │  │   Turso    │  │   │   │   │  │   Turso    │  │   │   │   │  │  PGlite    │  │   │
│   │  └────────────┘  │   │   │   │  └────────────┘  │   │   │   │  └────────────┘  │   │
│   └──────────────────┘   │   │   └──────────────────┘   │   │   └──────────────────┘   │
└──────────────────────────┘   └──────────────────────────┘   └──────────────────────────┘
```

## Core Components

### VTGate (Cloudflare Worker)

VTGate is the query router - it's the entry point for all client requests. Running as a Cloudflare Worker, it:

- **Parses SQL** to understand the query structure
- **Plans execution** based on VSchema configuration
- **Routes queries** to the appropriate shard(s)
- **Aggregates results** for cross-shard queries (COUNT, SUM, AVG, MIN, MAX)
- **Coordinates transactions** including two-phase commit for cross-shard writes

```typescript
// VTGate exported from your Worker
import { VTGate } from '@dotdo/vitess/server';

export default VTGate;
```

#### Query Planning

VTGate creates execution plans based on the query and VSchema:

| Plan Type | Description | Example |
|-----------|-------------|---------|
| `single_shard` | Query targets exactly one shard | `WHERE tenant_id = 'abc'` |
| `scatter` | Query must hit all shards | `SELECT * FROM users` |
| `scatter_aggregate` | Scatter with aggregation | `SELECT COUNT(*) FROM users` |
| `lookup` | Uses lookup vindex to find shard | `WHERE email = '...'` |
| `unsharded` | Unsharded keyspace | Reference tables |

### VTTablet (Durable Object)

VTTablet is the shard-level executor - one Durable Object per shard. It:

- **Executes queries** against the local storage engine
- **Manages transactions** (BEGIN, COMMIT, ROLLBACK)
- **Abstracts storage** (PostgreSQL or SQLite)
- **Handles prepared statements** (future)
- **Supports 2PC** for cross-shard transactions

```typescript
// VTTablet exported as a Durable Object
import { VTTablet } from '@dotdo/vitess/server';

export { VTTablet };
```

#### Storage Engine Abstraction

VTTablet uses a pluggable storage engine interface:

```typescript
interface StorageEngine {
  readonly type: 'pglite' | 'turso' | 'sqlite' | 'postgres';
  query<T>(sql: string, params?: unknown[]): Promise<QueryResult<T>>;
  execute(sql: string, params?: unknown[]): Promise<ExecuteResult>;
  beginTransaction(): Promise<TransactionHandle>;
  close(): Promise<void>;
}
```

### Storage Engines

#### PGlite (PostgreSQL)

Full PostgreSQL compatibility via WebAssembly:

```typescript
import { PGliteAdapter } from '@dotdo/vitess-postgres';

const engine = new PGliteAdapter({
  dataDir: '/data/shard-0',  // Persistent storage
  initSchema: 'CREATE TABLE ...',
});
await engine.init();
```

**Advantages:**
- Full PostgreSQL SQL compatibility
- JSONB support
- Rich type system
- Extensions (future)

#### Turso (SQLite/libSQL)

Lightweight SQLite with optional cloud sync:

```typescript
import { TursoAdapter } from '@dotdo/vitess-sqlite';

const engine = new TursoAdapter({
  url: 'file:./shard-0.db',
  dialect: 'postgres',  // Accept Postgres SQL
});
await engine.connect();
```

**Advantages:**
- Smaller footprint
- Faster cold starts
- Edge sync with Turso cloud
- PostgreSQL dialect translation

## Sharding Concepts

### Keyspaces

A keyspace is a logical database that can be sharded or unsharded:

```typescript
interface Keyspace {
  name: string;
  shardCount: number;
  engine: 'postgres' | 'sqlite';
}
```

### VSchema

VSchema defines how tables are sharded:

```typescript
const vschema: VSchema = {
  keyspace: 'main',
  sharded: true,
  tables: {
    users: {
      vindex: {
        type: 'hash',
        columns: ['tenant_id'],
      },
    },
    orders: {
      vindex: {
        type: 'hash',
        columns: ['tenant_id'],
      },
    },
  },
  vindexes: {
    tenant_hash: {
      type: 'hash',
      columns: ['tenant_id'],
    },
  },
};
```

### Vindexes

Vindexes (Virtual Indexes) determine how rows are mapped to shards:

| Type | Description | Use Case |
|------|-------------|----------|
| `hash` | Hash-based distribution | Even distribution |
| `consistent_hash` | Consistent hashing | Minimizes resharding |
| `range` | Range-based sharding | Time-series data |
| `lookup` | Lookup table for routing | Secondary indexes |
| `null` | No routing (unsharded) | Reference tables |

#### Hash Vindex Example

```typescript
// tenant_id = 'acme' hashes to keyspace ID 0x3f...
// With 4 shards (-40, 40-80, 80-c0, c0-):
//   0x3f... falls in -40 shard
```

#### Lookup Vindex Example

```typescript
// Find shard by email (non-sharding key):
// 1. Query lookup table: email -> tenant_id
// 2. Hash tenant_id to find shard
// 3. Route to that shard
```

## Query Flow

### Single-Shard Query

```
Client                  VTGate                  VTTablet
   │                       │                        │
   │ SELECT * FROM users   │                        │
   │ WHERE tenant_id = 'a' │                        │
   │──────────────────────▶│                        │
   │                       │                        │
   │                       │ Route: hash('a') → -80 │
   │                       │────────────────────────▶│
   │                       │                        │
   │                       │     Query Result       │
   │                       │◀────────────────────────│
   │                       │                        │
   │    Query Result       │                        │
   │◀──────────────────────│                        │
```

### Scatter Query

```
Client                  VTGate           VTTablet-0  VTTablet-1  VTTablet-2
   │                       │                  │           │           │
   │ SELECT COUNT(*)       │                  │           │           │
   │ FROM users            │                  │           │           │
   │──────────────────────▶│                  │           │           │
   │                       │                  │           │           │
   │                       │──Scatter Query──▶│           │           │
   │                       │─────────────────────────────▶│           │
   │                       │────────────────────────────────────────▶│
   │                       │                  │           │           │
   │                       │◀──count: 100─────│           │           │
   │                       │◀─────────count: 150──────────│           │
   │                       │◀──────────────────count: 75────────────│
   │                       │                  │           │           │
   │   count: 325          │ (Aggregate)      │           │           │
   │◀──────────────────────│                  │           │           │
```

### Cross-Shard Transaction (2PC)

```
Client                  VTGate           VTTablet-0  VTTablet-1
   │                       │                  │           │
   │ BEGIN                 │                  │           │
   │──────────────────────▶│                  │           │
   │                       │──BEGIN──────────▶│           │
   │                       │─────────────────────BEGIN───▶│
   │                       │                  │           │
   │ UPDATE shard-0        │                  │           │
   │──────────────────────▶│────────────────▶│           │
   │                       │                  │           │
   │ UPDATE shard-1        │                  │           │
   │──────────────────────▶│─────────────────────────────▶│
   │                       │                  │           │
   │ COMMIT                │                  │           │
   │──────────────────────▶│                  │           │
   │                       │──PREPARE────────▶│           │
   │                       │───────────────────PREPARE───▶│
   │                       │◀─OK──────────────│           │
   │                       │◀──────────────────OK─────────│
   │                       │                  │           │
   │                       │──COMMIT─────────▶│           │
   │                       │───────────────────COMMIT────▶│
   │                       │                  │           │
   │   OK                  │                  │           │
   │◀──────────────────────│                  │           │
```

## RPC Protocol

Communication uses CapnWeb RPC with JSON serialization:

### Request Format

```typescript
interface RpcMessage {
  type: MessageType;     // 0x01 = QUERY, 0x02 = EXECUTE, etc.
  id: string;            // Unique request ID
  timestamp: number;     // Unix timestamp
}

interface QueryRequest extends RpcMessage {
  type: MessageType.QUERY;
  sql: string;
  params?: unknown[];
  keyspace?: string;
  txId?: string;         // For transactional queries
}
```

### Response Format

```typescript
interface QueryResponse extends RpcMessage {
  type: MessageType.RESULT;
  result: QueryResult;
}

interface ErrorResponse extends RpcMessage {
  type: MessageType.ERROR;
  code: string;
  message: string;
  shard?: string;        // Which shard failed
  sqlState?: string;     // SQL state code
}
```

## Deployment

### Cloudflare Workers Configuration

```toml
# wrangler.toml
name = "vitess-gateway"
main = "src/index.ts"

[[durable_objects.bindings]]
name = "VT_TABLET"
class_name = "VTTablet"

[[migrations]]
tag = "v1"
new_classes = ["VTTablet"]
```

### Worker Entry Point

```typescript
// src/index.ts
import { VTGate, VTTablet } from '@dotdo/vitess/server';
import { PGliteAdapter } from '@dotdo/vitess-postgres';

export default VTGate;
export { VTTablet };
```

## Performance Considerations

### Query Routing

- **Include sharding key** in WHERE clause for single-shard routing
- **Avoid scatter queries** when possible
- **Use lookup vindexes** for frequently-queried non-sharding columns

### Storage Engine Choice

| Factor | PGlite | Turso |
|--------|--------|-------|
| Cold start | ~200ms | ~50ms |
| SQL compatibility | Full PostgreSQL | SQLite + translation |
| Memory usage | Higher | Lower |
| JSONB support | Native | JSON text |
| Edge sync | No | Yes (Turso cloud) |

### Transaction Performance

- **Single-shard transactions** are fast (no coordination)
- **Cross-shard transactions** require 2PC (additional latency)
- **Read-only transactions** can be optimized

## Summary

Vitess.do provides a production-grade distributed database architecture for Cloudflare Workers:

- **VTGate** handles query parsing, shard routing, and result aggregation
- **VTTablet** Durable Objects execute queries on individual shards
- **VSchema** declaratively defines how tables are sharded
- **Vindexes** determine how rows map to shards
- Both **PostgreSQL** and **SQLite** backends are supported with identical client APIs

## Related Documentation

- [Getting Started](./getting-started.md) - Installation and first queries
- [API Reference](./api.md) - Complete API documentation
- [Migration Guide](./migration.md) - Moving to vitess.do from other solutions
- [@dotdo/vitess](../packages/vitess/README.md) - Client API reference
- [@dotdo/vitess-rpc](../packages/vitess-rpc/README.md) - Protocol details
- [@dotdo/vitess-postgres](../packages/vitess-postgres/README.md) - PostgreSQL engine
- [@dotdo/vitess-sqlite](../packages/vitess-sqlite/README.md) - SQLite engine
