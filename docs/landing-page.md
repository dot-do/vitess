# Landing Page Copy for vitess.do

Content and copy for the vitess.do marketing website.

---

## Hero Section

### Headline
**Distributed Database Sharding for Cloudflare Workers**

### Subheadline
Vitess-style horizontal scaling meets edge computing. Run PostgreSQL or SQLite across unlimited Durable Objects with automatic query routing, cross-shard transactions, and zero configuration.

### Primary CTA
**Get Started Free**

### Secondary CTA
**View Documentation**

### Hero Code Example
```typescript
import { createClient } from '@dotdo/vitess';

const client = createClient({
  endpoint: 'https://my-app.vitess.do',
  keyspace: 'main',
});

// Automatic shard routing based on tenant_id
const orders = await client.query(
  'SELECT * FROM orders WHERE tenant_id = $1',
  [tenantId]
);

// Cross-shard aggregation, handled automatically
const total = await client.query(
  'SELECT SUM(amount) FROM orders WHERE status = $1',
  ['completed']
);
```

---

## Problem Statement Section

### Headline
**Single Durable Objects Hit a Wall**

### Body
Your Cloudflare Worker application is growing. A single Durable Object can only handle so much data and so many concurrent requests. You need horizontal scaling, but building your own sharding layer means:

- Writing complex shard routing logic
- Handling cross-shard transactions manually
- Building scatter-gather for aggregation queries
- Managing shard rebalancing and migrations
- Debugging distributed system failures

**There's a better way.**

---

## Solution Section

### Headline
**Automatic Sharding, Zero Friction**

### Subheadline
vitess.do brings battle-tested Vitess architecture to Cloudflare's edge. Define your schema once, and let us handle the rest.

### Feature Cards

#### Automatic Query Routing
Point queries route to exactly one shard. No manual shard key calculation needed.

```typescript
// VTGate automatically routes to the correct shard
const user = await client.query(
  'SELECT * FROM users WHERE id = $1',
  [userId]
);
```

#### Cross-Shard Transactions
ACID transactions across multiple shards with two-phase commit coordination.

```typescript
await client.transaction(async (tx) => {
  await tx.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, fromId]);
  await tx.execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, toId]);
});
```

#### Scatter-Gather Aggregation
COUNT, SUM, AVG, MIN, MAX automatically aggregate across all shards.

```typescript
const stats = await client.query(`
  SELECT status, COUNT(*) as count, SUM(amount) as total
  FROM orders
  GROUP BY status
`);
```

#### Dual Backend Support
Choose PostgreSQL (PGlite) for full SQL power or SQLite (Turso) for lightweight edge performance.

```typescript
// Same client API, different backends
const pgClient = createClient({ endpoint: 'https://pg.vitess.do' });
const sqliteClient = createClient({ endpoint: 'https://sqlite.vitess.do' });
```

---

## Architecture Section

### Headline
**Production-Grade Architecture**

### Diagram Description
```
Client SDK
    |
    v
VTGate (Worker)
    - Query parsing
    - Shard routing
    - Result aggregation
    |
    +---> VTTablet DO (Shard 0)
    |         |
    |         v
    |     PGlite / Turso
    |
    +---> VTTablet DO (Shard 1)
    |         |
    |         v
    |     PGlite / Turso
    |
    +---> VTTablet DO (Shard N)
              |
              v
          PGlite / Turso
```

### Architecture Points

- **VTGate**: Intelligent query router running in Cloudflare Workers
- **VTTablet**: Shard-level query executor in Durable Objects
- **VSchema**: Declarative sharding configuration
- **Vindexes**: Pluggable shard key algorithms (hash, range, lookup)

---

## Features Grid

### Horizontal Scaling
Scale from 1 to 1000+ shards without code changes. VSchema configuration drives automatic data distribution.

### Global Edge Deployment
Run your database at the edge in 300+ Cloudflare locations. Durable Objects provide strong consistency with low latency.

### Familiar SQL
Write standard PostgreSQL or SQLite queries. No new query language to learn.

### Type-Safe Client
Full TypeScript support with generic query results and auto-completion.

### Automatic Failover
Shard health monitoring with automatic traffic routing around failures.

### Real-Time Metrics
Query latencies, error rates, and shard distribution visible in your dashboard.

---

## Use Cases Section

### Multi-Tenant SaaS
Isolate tenant data across shards with `tenant_id` as the sharding key. Each tenant's queries hit exactly one shard.

```typescript
// All queries scoped to one shard
await client.query('SELECT * FROM orders WHERE tenant_id = $1', [tenantId]);
await client.query('SELECT * FROM invoices WHERE tenant_id = $1', [tenantId]);
```

### High-Volume Analytics
Aggregate petabytes of event data with scatter-gather queries that parallelize across shards.

```typescript
const result = await client.query(`
  SELECT
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) as events,
    COUNT(DISTINCT user_id) as users
  FROM events
  WHERE timestamp > NOW() - INTERVAL '24 hours'
  GROUP BY hour
`);
```

### Gaming & Real-Time Apps
Low-latency reads and writes with data locality. Shard by `game_id` or `region` for optimal performance.

```typescript
await client.transaction(async (tx) => {
  const state = await tx.query('SELECT * FROM game_state WHERE game_id = $1 FOR UPDATE', [gameId]);
  await tx.execute('UPDATE game_state SET score = $1 WHERE game_id = $2', [newScore, gameId]);
});
```

### E-Commerce
Handle flash sales with automatic load distribution. Transactions ensure inventory consistency.

```typescript
await client.transaction(async (tx) => {
  const item = await tx.query('SELECT qty FROM inventory WHERE sku = $1 FOR UPDATE', [sku]);
  if (item.rows[0].qty >= quantity) {
    await tx.execute('UPDATE inventory SET qty = qty - $1 WHERE sku = $2', [quantity, sku]);
    await tx.execute('INSERT INTO orders (sku, qty, user_id) VALUES ($1, $2, $3)', [sku, quantity, userId]);
  }
});
```

---

## Code Examples Section

### Quick Start

```bash
npm install @dotdo/vitess
```

```typescript
import { createClient } from '@dotdo/vitess';

const client = createClient({
  endpoint: 'https://my-app.vitess.do',
  keyspace: 'main',
  token: process.env.VITESS_TOKEN,
});

await client.connect();

// Type-safe queries
interface User {
  id: string;
  name: string;
  email: string;
}

const users = await client.query<User>(
  'SELECT * FROM users WHERE active = $1',
  [true]
);

for (const user of users.rows) {
  console.log(user.name); // TypeScript knows this is string
}

await client.disconnect();
```

### Define Your Schema

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
          "column_vindexes": [{
            "column": "tenant_id",
            "name": "tenant_hash"
          }]
        },
        "orders": {
          "column_vindexes": [{
            "column": "tenant_id",
            "name": "tenant_hash"
          }]
        }
      }
    }
  }
}
```

### Deploy Your Backend

```typescript
// worker.ts
import { VTGate, VTTablet } from '@dotdo/vitess/server';
import { PGliteAdapter } from '@dotdo/vitess-postgres';

export default VTGate;
export { VTTablet };
```

```toml
# wrangler.toml
name = "my-vitess-app"
main = "worker.ts"

[[durable_objects.bindings]]
name = "VTTABLET"
class_name = "VTTablet"
```

---

## Comparison Section

### Headline
**How vitess.do Compares**

| Feature | vitess.do | Raw PGlite | D1 | Planetscale |
|---------|-----------|------------|-----|-------------|
| Horizontal Sharding | Automatic | Manual | None | Automatic |
| Edge Deployment | 300+ locations | Single DO | Cloudflare | Centralized |
| Transaction Support | Cross-shard 2PC | Single-node | Single-DB | Single-shard |
| SQL Compatibility | Full PostgreSQL | Full PostgreSQL | SQLite | MySQL |
| Durable Objects | Native | Native | No | No |
| Query Routing | Automatic | Manual | N/A | Automatic |
| Pricing | Usage-based | DO costs | Per-query | Per-row |

---

## Pricing Section

### Headline
**Simple, Predictable Pricing**

### Free Tier
**$0/month**
- 100K queries/month
- 1 GB storage
- 2 shards
- Community support
- **Get Started Free**

### Pro
**$49/month**
- 10M queries/month
- 100 GB storage
- 16 shards
- Email support
- Custom domains
- **Start Pro Trial**

### Enterprise
**Custom pricing**
- Unlimited queries
- Unlimited storage
- Unlimited shards
- 24/7 support
- SLA guarantee
- Dedicated infrastructure
- **Contact Sales**

### Pricing Notes
- Queries are billed per request to VTGate
- Storage is measured at rest across all shards
- No egress fees
- No per-row charges

---

## Social Proof Section

### Headline
**Trusted by Edge-First Teams**

### Testimonials

> "We migrated from a custom sharding solution to vitess.do in a weekend. Our query routing code went from 500 lines to zero."
>
> **Sarah Chen**, CTO at TenantBase

> "vitess.do handles our Black Friday traffic without breaking a sweat. 10x normal load, same latency."
>
> **Marcus Johnson**, Platform Lead at ShopFast

> "Finally, a distributed database that understands Cloudflare Workers. The Durable Objects integration is seamless."
>
> **Alex Rivera**, Founder at EdgeDB Labs

### Metrics
- **50ms** average query latency
- **99.99%** uptime SLA
- **1M+** queries per second across our fleet
- **500+** companies building with vitess.do

---

## FAQ Section

### Headline
**Frequently Asked Questions**

#### How is vitess.do different from Vitess?
vitess.do is inspired by Vitess's architecture but built natively for Cloudflare Workers and Durable Objects. It supports both PostgreSQL (via PGlite) and SQLite (via Turso) backends, whereas Vitess supports only MySQL.

#### Can I use my existing PostgreSQL schema?
Yes. vitess.do with PGlite backend supports standard PostgreSQL SQL syntax, types, and features including JSONB, arrays, and window functions.

#### How do I choose between PGlite and Turso backends?
PGlite offers full PostgreSQL compatibility and is ideal for complex queries and strict type requirements. Turso (SQLite) has a smaller footprint and faster cold starts, making it better for simple workloads at massive scale.

#### What happens if a shard fails?
VTGate monitors shard health and automatically routes around failures. Queries to affected shards return errors, but the rest of your application continues working.

#### Can I migrate from a single Durable Object?
Yes. See our [Migration Guide](/docs/migration) for step-by-step instructions on moving from raw PGlite to vitess.do with sharding.

#### Is there vendor lock-in?
vitess.do uses standard SQL and open formats. You can export your data and run the same queries against any PostgreSQL or SQLite database.

---

## CTA Section

### Headline
**Start Building Today**

### Body
Get your first vitess.do cluster running in minutes. No credit card required.

### Primary CTA
**Get Started Free**

### Secondary CTA
**Schedule a Demo**

---

## Footer Links

### Product
- Features
- Pricing
- Documentation
- API Reference
- Changelog

### Resources
- Getting Started Guide
- Migration Guide
- Architecture Overview
- Best Practices
- Case Studies

### Company
- About
- Blog
- Careers
- Contact
- Press Kit

### Legal
- Terms of Service
- Privacy Policy
- Security
- SLA

### Community
- Discord
- GitHub
- Twitter
- Stack Overflow

---

## SEO Metadata

### Page Title
vitess.do - Distributed Database Sharding for Cloudflare Workers

### Meta Description
Horizontal database scaling for Cloudflare Workers. Automatic query routing, cross-shard transactions, and PostgreSQL/SQLite support. Get started free.

### Keywords
- cloudflare workers database
- durable objects sharding
- postgresql edge database
- distributed sqlite
- vitess cloudflare
- horizontal scaling workers
- multi-tenant database
- edge database sharding
