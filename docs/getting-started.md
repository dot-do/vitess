# Getting Started with Vitess.do

This guide walks you through installing Vitess.do, running your first query, and working with transactions. By the end, you will understand how to use both the client SDK and direct storage engine access.

## Prerequisites

- Node.js 18+ or Bun
- npm, pnpm, or yarn
- A Cloudflare account (for deployment)

## Installation

Install the main SDK:

```bash
npm install @dotdo/vitess
```

Choose and install a storage engine:

```bash
# PostgreSQL backend (PGlite - full Postgres compatibility)
npm install @dotdo/vitess-postgres

# SQLite backend (Turso/libSQL - lightweight, edge-ready)
npm install @dotdo/vitess-sqlite

# Or both for different use cases
npm install @dotdo/vitess-postgres @dotdo/vitess-sqlite
```

## Your First Query

### Client-Side Usage

```typescript
import { createClient } from '@dotdo/vitess';

async function main() {
  // 1. Create a client
  const client = createClient({
    endpoint: 'https://my-app.vitess.do',
    keyspace: 'main',
  });

  // 2. Connect to VTGate
  await client.connect();

  // 3. Execute a query
  const result = await client.query<{ id: number; name: string }>(
    'SELECT id, name FROM users WHERE status = $1',
    ['active']
  );

  console.log(`Found ${result.rowCount} users:`);
  for (const user of result.rows) {
    console.log(`  - ${user.name} (ID: ${user.id})`);
  }

  // 4. Disconnect
  await client.disconnect();
}

main().catch(console.error);
```

### Direct Storage Engine Usage

For testing or single-node deployments, you can use storage engines directly:

#### PostgreSQL (PGlite)

```typescript
import { PGliteAdapter } from '@dotdo/vitess-postgres';

async function main() {
  const db = new PGliteAdapter();
  await db.init();

  // Create a table
  await db.execute(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE
    )
  `);

  // Insert data
  const insertResult = await db.execute(
    'INSERT INTO users (name, email) VALUES ($1, $2)',
    ['Alice', 'alice@example.com']
  );
  console.log(`Inserted user with ID: ${insertResult.lastInsertId}`);

  // Query data
  const users = await db.query<{ id: number; name: string; email: string }>(
    'SELECT * FROM users'
  );
  console.log('Users:', users.rows);

  await db.close();
}

main().catch(console.error);
```

#### SQLite (Turso)

```typescript
import { TursoAdapter } from '@dotdo/vitess-sqlite';

async function main() {
  const db = new TursoAdapter({ url: ':memory:' });
  await db.connect();

  // Create a table
  await db.execute(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      email TEXT UNIQUE
    )
  `);

  // Insert data
  const insertResult = await db.execute(
    'INSERT INTO users (name, email) VALUES (?, ?)',
    ['Bob', 'bob@example.com']
  );
  console.log(`Inserted user with ID: ${insertResult.lastInsertId}`);

  // Query data
  const users = await db.query<{ id: number; name: string; email: string }>(
    'SELECT * FROM users'
  );
  console.log('Users:', users.rows);

  await db.close();
}

main().catch(console.error);
```

### Using PostgreSQL Dialect with SQLite

SQLite can accept PostgreSQL-style SQL with automatic translation:

```typescript
import { TursoAdapter } from '@dotdo/vitess-sqlite';

async function main() {
  // Enable postgres dialect mode
  const db = new TursoAdapter({
    url: ':memory:',
    dialect: 'postgres',
  });
  await db.connect();

  // Write PostgreSQL-style SQL - automatically translated
  await db.execute(`
    CREATE TABLE users (
      id SERIAL PRIMARY KEY,
      name VARCHAR(100) NOT NULL,
      active BOOLEAN DEFAULT TRUE,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  // PostgreSQL parameter style ($1, $2) works
  await db.execute(
    'INSERT INTO users (name, active) VALUES ($1, $2)',
    ['Charlie', true]
  );

  // PostgreSQL boolean literals work
  const activeUsers = await db.query(
    'SELECT * FROM users WHERE active = TRUE'
  );

  await db.close();
}

main().catch(console.error);
```

## Transactions

### Basic Transaction

```typescript
import { createClient } from '@dotdo/vitess';

const client = createClient({
  endpoint: 'https://my-app.vitess.do',
  keyspace: 'main',
});

await client.connect();

// Transaction with automatic commit/rollback
await client.transaction(async (tx) => {
  // All operations run in the same transaction
  await tx.execute(
    'UPDATE accounts SET balance = balance - $1 WHERE id = $2',
    [100, fromAccountId]
  );

  await tx.execute(
    'UPDATE accounts SET balance = balance + $1 WHERE id = $2',
    [100, toAccountId]
  );

  // If any operation fails, the entire transaction rolls back
});

await client.disconnect();
```

### Transaction with Return Value

```typescript
const orderId = await client.transaction(async (tx) => {
  // Check inventory
  const inventory = await tx.query<{ stock: number }>(
    'SELECT stock FROM products WHERE id = $1',
    [productId]
  );

  if (inventory.rows[0].stock < quantity) {
    throw new Error('Insufficient stock');
  }

  // Decrement stock
  await tx.execute(
    'UPDATE products SET stock = stock - $1 WHERE id = $2',
    [quantity, productId]
  );

  // Create order
  const result = await tx.execute(
    'INSERT INTO orders (product_id, quantity, user_id) VALUES ($1, $2, $3)',
    [productId, quantity, userId]
  );

  return result.lastInsertId;
});

console.log(`Created order: ${orderId}`);
```

### Transaction Options

```typescript
// Serializable isolation for strict consistency
await client.transaction(async (tx) => {
  // ...
}, { isolation: 'serializable' });

// Read-only transaction for better performance
await client.transaction(async (tx) => {
  const report = await tx.query('SELECT * FROM reports WHERE date = $1', [today]);
  return report.rows;
}, { readOnly: true });

// Transaction with timeout
await client.transaction(async (tx) => {
  // ...
}, { timeout: 5000 }); // 5 second timeout
```

### Direct Storage Engine Transactions

```typescript
import { PGliteAdapter } from '@dotdo/vitess-postgres';

const db = new PGliteAdapter();
await db.init();

// Method 1: Managed transaction
await db.transaction(async (tx) => {
  await tx.execute('INSERT INTO log (msg) VALUES ($1)', ['Started']);
  await tx.execute('INSERT INTO log (msg) VALUES ($1)', ['Completed']);
});

// Method 2: Manual transaction control
const tx = await db.begin();
try {
  await tx.execute('DELETE FROM temp_data WHERE expired = TRUE');
  await tx.execute('UPDATE stats SET last_cleanup = NOW()');
  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}

await db.close();
```

## Error Handling

```typescript
import { createClient, VitessError } from '@dotdo/vitess';

const client = createClient({ endpoint: 'https://my-app.vitess.do' });

try {
  await client.connect();
  await client.execute(
    'INSERT INTO users (email) VALUES ($1)',
    ['duplicate@example.com']
  );
} catch (error) {
  if (error instanceof VitessError) {
    console.error(`Vitess error: ${error.code}`);
    console.error(`Message: ${error.message}`);

    if (error.shard) {
      console.error(`Failed on shard: ${error.shard}`);
    }
  } else {
    console.error('Unexpected error:', error);
  }
}
```

## Next Steps

Now that you have run your first queries, explore the rest of the documentation:

- [Architecture Guide](./architecture.md) - Understand VTGate, VTTablet, and sharding concepts
- [API Reference](./api.md) - Complete API documentation
- [Migration Guide](./migration.md) - Migrate from other databases or single-node setups

For package-specific details:

- [@dotdo/vitess](../packages/vitess/README.md) - Main SDK
- [@dotdo/vitess-postgres](../packages/vitess-postgres/README.md) - PostgreSQL backend
- [@dotdo/vitess-sqlite](../packages/vitess-sqlite/README.md) - SQLite backend
- [@dotdo/vitess-rpc](../packages/vitess-rpc/README.md) - Protocol types

## Troubleshooting

### Connection Issues

```typescript
// Add retry configuration for unreliable networks
const client = createClient({
  endpoint: 'https://my-app.vitess.do',
  retry: {
    maxAttempts: 5,
    backoffMs: 500,
  },
  timeout: 60000,
});
```

### Type Safety

```typescript
// Define your row types for type-safe queries
interface User {
  id: number;
  name: string;
  email: string;
  created_at: Date;
}

const result = await client.query<User>(
  'SELECT * FROM users WHERE id = $1',
  [userId]
);

// result.rows is User[]
const user = result.rows[0];
console.log(user.name); // TypeScript knows this is a string
```

### Checking Connection Status

```typescript
if (!client.isConnected()) {
  await client.connect();
}

// Or use connect() - it's idempotent
await client.connect(); // Safe to call multiple times
```
