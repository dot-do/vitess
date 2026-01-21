/**
 * @dotdo/vitess
 *
 * Vitess-style distributed sharding client for Cloudflare Workers.
 * Supports both PostgreSQL (PGlite) and SQLite (Turso) backends.
 *
 * @example
 * ```ts
 * import { createClient } from '@dotdo/vitess';
 *
 * const client = createClient({
 *   endpoint: 'https://my-app.vitess.do',
 *   keyspace: 'main',
 * });
 *
 * await client.connect();
 *
 * // Query (works with both Postgres and SQLite backends)
 * const users = await client.query<User>('SELECT * FROM users WHERE tenant_id = $1', [tenantId]);
 *
 * // Transaction
 * await client.transaction(async (tx) => {
 *   await tx.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, fromId]);
 *   await tx.execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, toId]);
 * });
 *
 * await client.disconnect();
 * ```
 */

export { VitessClient, VitessError, createClient } from './client.js';
export type { VitessConfig, Transaction } from './client.js';

// Re-export common types from vitess-rpc
export type {
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
} from '@dotdo/vitess-rpc';
