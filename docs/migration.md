# Migration Guide

This guide covers migrating to vitess.do from various starting points.

## Table of Contents

- [From Raw PGlite to vitess.do](#from-raw-pglite-to-vitessdo)
- [From Single-Node to Sharded](#from-single-node-to-sharded)
- [From Other Sharding Solutions](#from-other-sharding-solutions)
  - [From Citus](#from-citus)
  - [From Custom Sharding](#from-custom-sharding)
  - [From MongoDB](#from-mongodb)
- [SQL Compatibility](#sql-compatibility)
- [Schema Migration Strategies](#schema-migration-strategies)

---

## From Raw PGlite to vitess.do

If you're currently using PGlite directly in your Cloudflare Workers, migrating to vitess.do provides distributed sharding, automatic routing, and transaction coordination.

### Before: Raw PGlite

```typescript
import { PGlite } from '@electric-sql/pglite';

export default {
  async fetch(request: Request, env: Env) {
    const db = new PGlite();
    await db.waitReady;

    // Direct queries - no sharding
    const result = await db.query(
      'SELECT * FROM users WHERE id = $1',
      [userId]
    );

    return Response.json(result.rows);
  }
};
```

### After: vitess.do

```typescript
import { createClient } from '@dotdo/vitess';

export default {
  async fetch(request: Request, env: Env) {
    const client = createClient({
      endpoint: env.VITESS_ENDPOINT,
      keyspace: 'main',
      token: env.VITESS_TOKEN,
    });

    await client.connect();

    // Same query syntax - automatic shard routing
    const result = await client.query<User>(
      'SELECT * FROM users WHERE id = $1',
      [userId]
    );

    await client.disconnect();
    return Response.json(result.rows);
  }
};
```

### Migration Steps

1. **Install vitess.do packages:**
   ```bash
   npm install @dotdo/vitess
   ```

2. **Deploy VTGate and VTTablet Workers** (or use managed vitess.do):
   ```typescript
   // wrangler.toml
   [[durable_objects.bindings]]
   name = "VTTABLET"
   class_name = "VTTablet"
   ```

3. **Update imports and client initialization:**
   ```typescript
   // Before
   import { PGlite } from '@electric-sql/pglite';
   const db = new PGlite();

   // After
   import { createClient } from '@dotdo/vitess';
   const client = createClient({ endpoint, keyspace });
   ```

4. **Replace query calls:**
   ```typescript
   // Before
   const result = await db.query(sql, params);
   const rows = result.rows;

   // After
   const result = await client.query(sql, params);
   const rows = result.rows; // Same structure!
   ```

5. **Update write operations:**
   ```typescript
   // Before
   const result = await db.exec(sql, params);

   // After
   const result = await client.execute(sql, params);
   console.log(result.affected); // Number of affected rows
   ```

6. **Migrate transactions:**
   ```typescript
   // Before (manual BEGIN/COMMIT)
   await db.exec('BEGIN');
   await db.exec('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, from]);
   await db.exec('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, to]);
   await db.exec('COMMIT');

   // After (automatic commit/rollback)
   await client.transaction(async (tx) => {
     await tx.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, from]);
     await tx.execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, to]);
   });
   ```

### Key Differences

| Aspect | Raw PGlite | vitess.do |
|--------|------------|-----------|
| Sharding | None (single instance) | Automatic via VSchema |
| Scaling | Limited by single DO | Scales across shards |
| Transactions | Manual BEGIN/COMMIT | Automatic with callback |
| Cross-shard queries | N/A | Automatic scatter-gather |
| Connection | Direct to PGlite | Via VTGate |

---

## From Single-Node to Sharded

Converting an unsharded database to a sharded one requires planning your sharding key and VSchema.

### Step 1: Analyze Your Data

Identify a good sharding key:

```sql
-- Find high-cardinality columns
SELECT
  COUNT(DISTINCT tenant_id) as tenants,
  COUNT(DISTINCT user_id) as users,
  COUNT(*) as total_rows
FROM orders;

-- Check data distribution
SELECT
  tenant_id,
  COUNT(*) as order_count
FROM orders
GROUP BY tenant_id
ORDER BY order_count DESC
LIMIT 20;
```

**Good sharding keys:**
- `tenant_id` - Multi-tenant applications
- `user_id` - User-centric data
- `region_id` - Geographic distribution
- Composite keys for complex patterns

**Avoid:**
- Auto-increment IDs alone (hotspots)
- Timestamps (range queries become scatter)
- Low-cardinality columns

### Step 2: Define Your VSchema

```json
{
  "keyspaces": {
    "main": {
      "sharded": true,
      "vindexes": {
        "tenant_hash": {
          "type": "hash"
        }
      },
      "tables": {
        "users": {
          "column_vindexes": [
            {
              "column": "tenant_id",
              "name": "tenant_hash"
            }
          ]
        },
        "orders": {
          "column_vindexes": [
            {
              "column": "tenant_id",
              "name": "tenant_hash"
            }
          ]
        },
        "order_items": {
          "column_vindexes": [
            {
              "column": "tenant_id",
              "name": "tenant_hash"
            }
          ]
        }
      }
    }
  }
}
```

### Step 3: Update Your Queries

Ensure queries include the sharding key:

```typescript
// Before: Full table scan (will scatter)
const result = await client.query(
  'SELECT * FROM orders WHERE status = $1',
  ['pending']
);

// After: Single-shard query
const result = await client.query(
  'SELECT * FROM orders WHERE tenant_id = $1 AND status = $2',
  [tenantId, 'pending']
);
```

### Step 4: Handle Cross-Shard Operations

Some operations will naturally scatter:

```typescript
// This scatters to all shards (but vitess.do handles it)
const result = await client.query(
  'SELECT COUNT(*) FROM orders WHERE status = $1',
  ['pending']
);
// VTGate aggregates counts from all shards

// For complex aggregations, consider pre-aggregated tables
await client.execute(
  'INSERT INTO daily_stats (date, tenant_id, order_count) VALUES ($1, $2, $3) ON CONFLICT (date, tenant_id) DO UPDATE SET order_count = order_count + 1',
  [today, tenantId, 1]
);
```

### Step 5: Migrate Data

Use a phased approach:

```typescript
// 1. Create new sharded tables
await client.execute(`
  CREATE TABLE orders_sharded (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,  -- Sharding key
    user_id UUID NOT NULL,
    total DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT NOW()
  )
`);

// 2. Migrate data in batches
async function migrateBatch(lastId: string | null, batchSize: number) {
  const oldData = await legacyDb.query(`
    SELECT * FROM orders
    WHERE ($1::uuid IS NULL OR id > $1)
    ORDER BY id
    LIMIT $2
  `, [lastId, batchSize]);

  for (const row of oldData.rows) {
    await client.execute(
      'INSERT INTO orders_sharded (id, tenant_id, user_id, total, created_at) VALUES ($1, $2, $3, $4, $5)',
      [row.id, row.tenant_id, row.user_id, row.total, row.created_at]
    );
  }

  return oldData.rows[oldData.rows.length - 1]?.id;
}

// 3. Run migration
let lastId = null;
while (true) {
  lastId = await migrateBatch(lastId, 1000);
  if (!lastId) break;
}
```

---

## From Other Sharding Solutions

### From Citus

Citus uses a similar sharding model to Vitess. Migration is straightforward.

**Citus:**
```sql
-- Citus: Distributed table
SELECT create_distributed_table('orders', 'tenant_id');

-- Query
SELECT * FROM orders WHERE tenant_id = 123;
```

**vitess.do:**
```typescript
// VSchema defines the same sharding
const vschema = {
  keyspaces: {
    main: {
      sharded: true,
      tables: {
        orders: {
          column_vindexes: [{
            column: 'tenant_id',
            name: 'tenant_hash'
          }]
        }
      }
    }
  }
};

// Same query
const result = await client.query(
  'SELECT * FROM orders WHERE tenant_id = $1',
  [123]
);
```

**Key differences:**
- No `create_distributed_table` - VSchema handles it
- Reference tables (small, replicated) work differently
- Cross-shard joins handled by VTGate, not Citus coordinator

### From Custom Sharding

If you implemented custom sharding logic, vitess.do simplifies your code.

**Before: Custom sharding:**
```typescript
function getShardForTenant(tenantId: string): number {
  const hash = murmurhash(tenantId);
  return hash % NUM_SHARDS;
}

async function queryOrders(tenantId: string) {
  const shardId = getShardForTenant(tenantId);
  const db = getDbForShard(shardId);
  return db.query('SELECT * FROM orders WHERE tenant_id = $1', [tenantId]);
}

// Cross-shard queries required manual scatter-gather
async function countAllOrders() {
  const promises = Array.from({ length: NUM_SHARDS }, (_, i) =>
    getDbForShard(i).query('SELECT COUNT(*) as count FROM orders')
  );
  const results = await Promise.all(promises);
  return results.reduce((sum, r) => sum + r.rows[0].count, 0);
}
```

**After: vitess.do:**
```typescript
// Sharding logic is handled by VSchema + VTGate
async function queryOrders(tenantId: string) {
  return client.query(
    'SELECT * FROM orders WHERE tenant_id = $1',
    [tenantId]
  );
}

// Cross-shard aggregation is automatic
async function countAllOrders() {
  const result = await client.query('SELECT COUNT(*) as count FROM orders');
  return result.rows[0].count;
}
```

### From MongoDB

MongoDB's sharding concepts map to vitess.do:

| MongoDB | vitess.do |
|---------|-----------|
| Shard key | Vindex column |
| mongos | VTGate |
| mongod | VTTablet |
| Config servers | VSchema |
| Chunks | Shards |

**MongoDB:**
```javascript
// Sharding setup
sh.enableSharding("mydb");
sh.shardCollection("mydb.orders", { tenant_id: "hashed" });

// Query
db.orders.find({ tenant_id: "abc123" });

// Aggregation
db.orders.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } }
]);
```

**vitess.do:**
```typescript
// VSchema equivalent
const vschema = {
  keyspaces: {
    mydb: {
      sharded: true,
      vindexes: {
        tenant_hash: { type: 'hash' }
      },
      tables: {
        orders: {
          column_vindexes: [{
            column: 'tenant_id',
            name: 'tenant_hash'
          }]
        }
      }
    }
  }
};

// Query
const result = await client.query(
  'SELECT * FROM orders WHERE tenant_id = $1',
  ['abc123']
);

// Aggregation (SQL)
const stats = await client.query(`
  SELECT status, COUNT(*) as count
  FROM orders
  GROUP BY status
`);
```

---

## SQL Compatibility

### PostgreSQL to vitess.do

vitess.do with PGlite backend supports full PostgreSQL syntax:

```sql
-- All supported
SELECT * FROM users WHERE id = $1;
INSERT INTO users (name, data) VALUES ($1, $2::jsonb);
UPDATE users SET updated_at = NOW() WHERE id = $1;
DELETE FROM users WHERE inactive_since < NOW() - INTERVAL '30 days';

-- Window functions
SELECT *, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY created_at) as rn
FROM orders;

-- CTEs
WITH recent_orders AS (
  SELECT * FROM orders WHERE created_at > NOW() - INTERVAL '7 days'
)
SELECT user_id, COUNT(*) FROM recent_orders GROUP BY user_id;

-- JSONB
SELECT * FROM users WHERE data->>'role' = 'admin';
INSERT INTO events (payload) VALUES ($1::jsonb);
```

### SQLite Dialect Mode

vitess.do with Turso backend can accept PostgreSQL syntax:

```typescript
const adapter = new TursoAdapter({
  url: 'libsql://db.turso.io',
  authToken: token,
  dialect: 'postgres',  // Enable translation
});

// Write PostgreSQL syntax
await adapter.execute(
  'INSERT INTO users (name, active) VALUES ($1, TRUE)',
  ['Alice']
);

// Automatically translated to SQLite:
// INSERT INTO users (name, active) VALUES (?, 1)
```

**Auto-translated features:**

| PostgreSQL | SQLite Translation |
|------------|-------------------|
| `$1, $2, ...` | `?, ?, ...` |
| `SERIAL PRIMARY KEY` | `INTEGER PRIMARY KEY AUTOINCREMENT` |
| `BOOLEAN` | `INTEGER` (0/1) |
| `TRUE / FALSE` | `1 / 0` |
| `TIMESTAMP` | `TEXT` |
| `VARCHAR(n)` | `TEXT` |
| `JSONB` | `TEXT` |
| `NOW()` | `datetime('now')` |
| `ILIKE` | `LIKE` |
| `::type` casts | `CAST(... AS type)` |

---

## Schema Migration Strategies

### Blue-Green Deployment

1. Deploy new schema alongside old:
   ```sql
   CREATE TABLE users_v2 (
     id UUID PRIMARY KEY,
     tenant_id UUID NOT NULL,  -- New sharding key
     email TEXT UNIQUE,
     name TEXT
   );
   ```

2. Dual-write during migration:
   ```typescript
   await client.transaction(async (tx) => {
     await tx.execute('INSERT INTO users_v1 ...', params);
     await tx.execute('INSERT INTO users_v2 ...', params);
   });
   ```

3. Switch reads to new schema:
   ```typescript
   const result = await client.query('SELECT * FROM users_v2 WHERE tenant_id = $1', [tenantId]);
   ```

4. Drop old schema after validation.

### Online Schema Changes

For minimal downtime, use incremental changes:

```typescript
// 1. Add new column (nullable)
await client.execute('ALTER TABLE users ADD COLUMN tenant_id UUID');

// 2. Backfill data
await client.execute(`
  UPDATE users SET tenant_id = (
    SELECT tenant_id FROM tenants WHERE tenants.id = users.tenant_ref_id
  )
  WHERE tenant_id IS NULL
`);

// 3. Add constraint
await client.execute('ALTER TABLE users ALTER COLUMN tenant_id SET NOT NULL');

// 4. Update VSchema to use new sharding key
```

### Resharding

To change the number of shards:

1. **Plan new shard ranges:**
   ```
   Current: [-80, 80-]     (2 shards)
   Target:  [-40, 40-80, 80-c0, c0-]  (4 shards)
   ```

2. **Create new VTTablet DOs for new shards**

3. **Split data using keyspace ID ranges:**
   ```typescript
   // VTGate handles routing during split
   // Data is migrated in background
   ```

4. **Update VSchema with new shard ranges**

5. **Validate and cutover**

---

## Checklist

Before going live with vitess.do:

- [ ] Sharding key chosen and tested for distribution
- [ ] VSchema defined for all tables
- [ ] Queries updated to include sharding key where possible
- [ ] Cross-shard queries identified and optimized
- [ ] Transactions tested across multiple shards
- [ ] Error handling implemented for shard failures
- [ ] Monitoring and alerting configured
- [ ] Rollback plan documented
- [ ] Load testing completed
