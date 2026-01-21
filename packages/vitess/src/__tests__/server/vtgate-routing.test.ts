/**
 * VTGate Routing Tests - Single Shard Query Routing
 *
 * TDD Red tests for VTGate single-shard query routing based on vindex.
 * These tests define the expected behavior for routing queries to a single shard
 * when the query contains a sharding key that can be resolved to one shard.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  VTGate,
  createVTGate,
  type VTGateConfig,
  type ShardRoute,
  type QueryPlan,
} from '../../server/vtgate.js';
import { createVSchemaBuilder, type VSchema } from '../../server/vschema.js';
import { HashVindex, createVindex } from '../../server/vindexes.js';

describe('VTGate Single-Shard Routing', () => {
  let vtgate: VTGate;
  let vschema: VSchema;

  beforeEach(() => {
    // Setup a basic sharded VSchema with hash vindex
    vschema = createVSchemaBuilder()
      .addKeyspace('commerce', true)
      .addVindex('commerce', 'hash', { type: 'hash' })
      .addTable('commerce', 'users', {
        column_vindexes: [{ column: 'id', name: 'hash' }],
      })
      .addTable('commerce', 'orders', {
        column_vindexes: [{ column: 'user_id', name: 'hash' }],
      })
      .setShards('commerce', ['-80', '80-'])
      .build();

    vtgate = createVTGate({
      vschema,
      shards: new Map([['commerce', ['-80', '80-']]]),
    });
  });

  describe('route()', () => {
    it('should route SELECT with = on sharding key to single shard', () => {
      const route = vtgate.route('SELECT * FROM users WHERE id = $1', [123]);

      expect(route.keyspace).toBe('commerce');
      expect(route.shards).toHaveLength(1);
      expect(route.scatter).toBe(false);
    });

    it('should route SELECT with IN clause on single value to single shard', () => {
      const route = vtgate.route('SELECT * FROM users WHERE id IN ($1)', [123]);

      expect(route.shards).toHaveLength(1);
      expect(route.scatter).toBe(false);
    });

    it('should route INSERT to single shard based on primary vindex column', () => {
      const route = vtgate.route(
        'INSERT INTO users (id, name, email) VALUES ($1, $2, $3)',
        [456, 'Alice', 'alice@example.com']
      );

      expect(route.shards).toHaveLength(1);
      expect(route.scatter).toBe(false);
    });

    it('should route UPDATE with sharding key to single shard', () => {
      const route = vtgate.route(
        'UPDATE users SET name = $2 WHERE id = $1',
        [123, 'Bob']
      );

      expect(route.shards).toHaveLength(1);
      expect(route.scatter).toBe(false);
    });

    it('should route DELETE with sharding key to single shard', () => {
      const route = vtgate.route('DELETE FROM users WHERE id = $1', [123]);

      expect(route.shards).toHaveLength(1);
      expect(route.scatter).toBe(false);
    });

    it('should route join query with both tables on same shard key', () => {
      const route = vtgate.route(
        `SELECT u.*, o.* FROM users u
         JOIN orders o ON u.id = o.user_id
         WHERE u.id = $1`,
        [123]
      );

      expect(route.shards).toHaveLength(1);
      expect(route.scatter).toBe(false);
    });

    it('should use parameter value to determine target shard', () => {
      // Different parameter values should route to potentially different shards
      const route1 = vtgate.route('SELECT * FROM users WHERE id = $1', [100]);
      const route2 = vtgate.route('SELECT * FROM users WHERE id = $1', [999]);

      // Both should be single-shard routes
      expect(route1.shards).toHaveLength(1);
      expect(route2.shards).toHaveLength(1);
      // They may or may not be the same shard depending on hash
      expect(route1.scatter).toBe(false);
      expect(route2.scatter).toBe(false);
    });

    it('should route to unsharded keyspace for unsharded tables', () => {
      const unshardedSchema = createVSchemaBuilder()
        .addKeyspace('lookup', false) // unsharded
        .addTable('lookup', 'countries', {})
        .build();

      const unshardedGate = createVTGate({
        vschema: unshardedSchema,
        shards: new Map([['lookup', ['-']]]),
      });

      const route = unshardedGate.route('SELECT * FROM countries WHERE code = $1', ['US']);

      expect(route.keyspace).toBe('lookup');
      expect(route.shards).toEqual(['-']);
      expect(route.scatter).toBe(false);
    });

    it('should handle compound primary key routing', () => {
      const compoundSchema = createVSchemaBuilder()
        .addKeyspace('multitenant', true)
        .addVindex('multitenant', 'tenant_hash', { type: 'hash' })
        .addTable('multitenant', 'documents', {
          column_vindexes: [{ column: 'tenant_id', name: 'tenant_hash' }],
        })
        .setShards('multitenant', ['-40', '40-80', '80-c0', 'c0-'])
        .build();

      const mtGate = createVTGate({
        vschema: compoundSchema,
        shards: new Map([['multitenant', ['-40', '40-80', '80-c0', 'c0-']]]),
      });

      const route = mtGate.route(
        'SELECT * FROM documents WHERE tenant_id = $1 AND doc_id = $2',
        ['tenant-123', 'doc-456']
      );

      expect(route.shards).toHaveLength(1);
      expect(route.scatter).toBe(false);
    });
  });

  describe('plan()', () => {
    it('should generate single_shard plan for point query', () => {
      const plan = vtgate.plan('SELECT * FROM users WHERE id = $1', [123]);

      expect(plan.type).toBe('single_shard');
      expect(plan.keyspace).toBe('commerce');
      expect(plan.table).toBe('users');
      expect(plan.shards).toHaveLength(1);
    });

    it('should generate unsharded plan for unsharded keyspace', () => {
      const unshardedSchema = createVSchemaBuilder()
        .addKeyspace('config', false)
        .addTable('config', 'settings', {})
        .build();

      const gate = createVTGate({
        vschema: unshardedSchema,
        shards: new Map([['config', ['-']]]),
      });

      const plan = gate.plan('SELECT * FROM settings WHERE key = $1', ['timeout']);

      expect(plan.type).toBe('unsharded');
    });

    it('should include SQL and params in plan', () => {
      const sql = 'SELECT * FROM users WHERE id = $1';
      const params = [123];
      const plan = vtgate.plan(sql, params);

      expect(plan.sql).toBe(sql);
      expect(plan.params).toEqual(params);
    });

    it('should generate lookup plan for secondary vindex', () => {
      const lookupSchema = createVSchemaBuilder()
        .addKeyspace('commerce', true)
        .addVindex('commerce', 'hash', { type: 'hash' })
        .addVindex('commerce', 'email_lookup', {
          type: 'lookup_unique',
          table: 'email_to_id',
          from: ['email'],
          to: 'user_id',
        })
        .addTable('commerce', 'users', {
          column_vindexes: [
            { column: 'id', name: 'hash' },
            { column: 'email', name: 'email_lookup' },
          ],
        })
        .setShards('commerce', ['-80', '80-'])
        .build();

      const gate = createVTGate({
        vschema: lookupSchema,
        shards: new Map([['commerce', ['-80', '80-']]]),
      });

      const plan = gate.plan('SELECT * FROM users WHERE email = $1', ['alice@example.com']);

      expect(plan.type).toBe('lookup');
    });
  });

  describe('execute() - single shard', () => {
    it('should execute query on single shard and return results', async () => {
      // Mock tablet setup would be needed here
      const result = await vtgate.execute('SELECT * FROM users WHERE id = $1', [123]);

      expect(result.rows).toBeDefined();
      expect(Array.isArray(result.rows)).toBe(true);
      expect(result.fields).toBeDefined();
    });

    it('should return rowCount for SELECT queries', async () => {
      const result = await vtgate.execute('SELECT * FROM users WHERE id = $1', [123]);

      expect(typeof result.rowCount).toBe('number');
      expect(result.rowCount).toBeGreaterThanOrEqual(0);
    });

    it('should return affected rows for INSERT', async () => {
      const result = await vtgate.execute(
        'INSERT INTO users (id, name) VALUES ($1, $2)',
        [456, 'Alice']
      );

      expect(result.rowCount).toBeGreaterThanOrEqual(0);
    });

    it('should return affected rows for UPDATE', async () => {
      const result = await vtgate.execute(
        'UPDATE users SET name = $2 WHERE id = $1',
        [123, 'Bob']
      );

      expect(result.rowCount).toBeGreaterThanOrEqual(0);
    });

    it('should return affected rows for DELETE', async () => {
      const result = await vtgate.execute('DELETE FROM users WHERE id = $1', [123]);

      expect(result.rowCount).toBeGreaterThanOrEqual(0);
    });
  });

  describe('Shard key extraction', () => {
    it('should extract shard key from WHERE clause with =', () => {
      const route = vtgate.route('SELECT * FROM users WHERE id = $1', [123]);

      // The route should contain exactly one shard
      expect(route.shards).toHaveLength(1);
    });

    it('should extract shard key from INSERT values', () => {
      const route = vtgate.route(
        'INSERT INTO users (id, name) VALUES ($1, $2)',
        [123, 'Alice']
      );

      expect(route.shards).toHaveLength(1);
    });

    it('should handle string shard keys', () => {
      const uuidSchema = createVSchemaBuilder()
        .addKeyspace('documents', true)
        .addVindex('documents', 'xxhash', {
          type: 'hash',
          params: { hash_function: 'xxhash' },
        })
        .addTable('documents', 'files', {
          column_vindexes: [{ column: 'uuid', name: 'xxhash' }],
        })
        .setShards('documents', ['-80', '80-'])
        .build();

      const gate = createVTGate({
        vschema: uuidSchema,
        shards: new Map([['documents', ['-80', '80-']]]),
      });

      const route = gate.route(
        'SELECT * FROM files WHERE uuid = $1',
        ['550e8400-e29b-41d4-a716-446655440000']
      );

      expect(route.shards).toHaveLength(1);
      expect(route.scatter).toBe(false);
    });

    it('should handle bigint shard keys', () => {
      const route = vtgate.route('SELECT * FROM users WHERE id = $1', [BigInt('9223372036854775807')]);

      expect(route.shards).toHaveLength(1);
      expect(route.scatter).toBe(false);
    });
  });

  describe('Error handling', () => {
    it('should throw error for unknown table', () => {
      expect(() => {
        vtgate.route('SELECT * FROM nonexistent WHERE id = $1', [123]);
      }).toThrow(/table.*not found/i);
    });

    it('should throw error for unknown keyspace', () => {
      expect(() => {
        vtgate.route('SELECT * FROM unknown_keyspace.users WHERE id = $1', [123]);
      }).toThrow(/keyspace.*not found/i);
    });

    it('should throw error when sharding key is missing for DML', () => {
      expect(() => {
        vtgate.route('INSERT INTO users (name) VALUES ($1)', ['Alice']);
      }).toThrow(/sharding key.*required/i);
    });

    it('should throw error for unsupported SQL syntax', () => {
      expect(() => {
        vtgate.route('TRUNCATE TABLE users', []);
      }).toThrow(/unsupported/i);
    });
  });

  describe('Performance', () => {
    it('should route queries in under 1ms for simple cases', () => {
      const start = performance.now();

      for (let i = 0; i < 1000; i++) {
        vtgate.route('SELECT * FROM users WHERE id = $1', [i]);
      }

      const elapsed = performance.now() - start;
      const perQuery = elapsed / 1000;

      expect(perQuery).toBeLessThan(1); // Less than 1ms per query
    });
  });
});
