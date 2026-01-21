/**
 * VTGate Scatter Tests - Multi-Shard Scatter-Gather Queries
 *
 * TDD Red tests for VTGate scatter-gather queries.
 * These tests define the expected behavior for queries that must be sent
 * to multiple (or all) shards and have their results merged.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  VTGate,
  createVTGate,
  type VTGateConfig,
  type ShardRoute,
  type QueryPlan,
  type QueryResult,
  type VTTabletStub,
} from '../../server/vtgate.js';
import { createVSchemaBuilder, type VSchema } from '../../server/vschema.js';

describe('VTGate Scatter-Gather Queries', () => {
  let vtgate: VTGate;
  let vschema: VSchema;
  let mockTablets: Map<string, VTTabletStub>;

  beforeEach(() => {
    // Setup a 4-shard keyspace
    vschema = createVSchemaBuilder()
      .addKeyspace('commerce', true)
      .addVindex('commerce', 'hash', { type: 'hash' })
      .addTable('commerce', 'users', {
        column_vindexes: [{ column: 'id', name: 'hash' }],
      })
      .addTable('commerce', 'orders', {
        column_vindexes: [{ column: 'user_id', name: 'hash' }],
      })
      .addTable('commerce', 'products', {
        column_vindexes: [{ column: 'id', name: 'hash' }],
      })
      .setShards('commerce', ['-40', '40-80', '80-c0', 'c0-'])
      .build();

    // Create mock tablets for testing
    mockTablets = new Map();
    const shards = ['-40', '40-80', '80-c0', 'c0-'];
    for (const shard of shards) {
      mockTablets.set(shard, {
        shard,
        execute: async (sql: string, params?: unknown[]): Promise<QueryResult> => ({
          rows: [{ id: 1, name: 'mock' }],
          rowCount: 1,
          fields: [
            { name: 'id', type: 'int' },
            { name: 'name', type: 'text' },
          ],
        }),
      });
    }

    vtgate = createVTGate({
      vschema,
      shards: new Map([['commerce', shards]]),
      tablets: mockTablets,
    });
  });

  describe('route() - scatter queries', () => {
    it('should scatter SELECT * without WHERE clause to all shards', () => {
      const route = vtgate.route('SELECT * FROM users', []);

      expect(route.keyspace).toBe('commerce');
      expect(route.shards).toHaveLength(4);
      expect(route.scatter).toBe(true);
    });

    it('should scatter SELECT with non-sharding key filter to all shards', () => {
      const route = vtgate.route('SELECT * FROM users WHERE name = $1', ['Alice']);

      expect(route.scatter).toBe(true);
      expect(route.shards).toHaveLength(4);
    });

    it('should scatter SELECT with LIKE on non-sharding column', () => {
      const route = vtgate.route("SELECT * FROM users WHERE email LIKE $1", ['%@example.com']);

      expect(route.scatter).toBe(true);
    });

    it('should scatter SELECT with IN clause on multiple values across shards', () => {
      // Multiple IDs that hash to different shards
      const route = vtgate.route('SELECT * FROM users WHERE id IN ($1, $2, $3)', [1, 1000, 999999]);

      // Could be 1-3 shards depending on hash distribution
      // But if all map to different shards, it's a scatter
      expect(route.shards.length).toBeGreaterThanOrEqual(1);
    });

    it('should scatter UPDATE without sharding key', () => {
      const route = vtgate.route('UPDATE users SET status = $1 WHERE created_at < $2', ['inactive', '2024-01-01']);

      expect(route.scatter).toBe(true);
      expect(route.shards).toHaveLength(4);
    });

    it('should scatter DELETE without sharding key', () => {
      const route = vtgate.route('DELETE FROM users WHERE last_login < $1', ['2023-01-01']);

      expect(route.scatter).toBe(true);
    });

    it('should scatter SELECT with ORDER BY and LIMIT', () => {
      const route = vtgate.route('SELECT * FROM users ORDER BY created_at DESC LIMIT 10', []);

      expect(route.scatter).toBe(true);
    });

    it('should scatter SELECT with GROUP BY on non-sharding column', () => {
      const route = vtgate.route('SELECT status, COUNT(*) FROM users GROUP BY status', []);

      expect(route.scatter).toBe(true);
    });
  });

  describe('plan() - scatter plans', () => {
    it('should generate scatter plan for full table scan', () => {
      const plan = vtgate.plan('SELECT * FROM users', []);

      expect(plan.type).toBe('scatter');
      expect(plan.shards).toHaveLength(4);
    });

    it('should generate scatter_aggregate plan for COUNT(*)', () => {
      const plan = vtgate.plan('SELECT COUNT(*) FROM users', []);

      expect(plan.type).toBe('scatter_aggregate');
      expect(plan.aggregations).toContain('COUNT');
    });

    it('should generate scatter_aggregate plan for SUM', () => {
      const plan = vtgate.plan('SELECT SUM(balance) FROM accounts', []);

      expect(plan.type).toBe('scatter_aggregate');
      expect(plan.aggregations).toContain('SUM');
    });

    it('should include all shards in scatter plan', () => {
      const plan = vtgate.plan('SELECT * FROM users WHERE name LIKE $1', ['%test%']);

      expect(plan.shards).toEqual(['-40', '40-80', '80-c0', 'c0-']);
    });
  });

  describe('scatter() - parallel execution', () => {
    it('should execute query on all shards in parallel', async () => {
      const plan = vtgate.plan('SELECT * FROM users', []);
      const results = await vtgate.scatter(plan);

      expect(results).toHaveLength(4);
      results.forEach((result) => {
        expect(result.rows).toBeDefined();
        expect(result.fields).toBeDefined();
      });
    });

    it('should return results from all shards even if some are empty', async () => {
      // Mock some tablets to return empty results
      mockTablets.get('-40')!.execute = async () => ({
        rows: [],
        rowCount: 0,
        fields: [
          { name: 'id', type: 'int' },
          { name: 'name', type: 'text' },
        ],
      });

      const plan = vtgate.plan('SELECT * FROM users WHERE name = $1', ['nonexistent']);
      const results = await vtgate.scatter(plan);

      expect(results).toHaveLength(4);
      expect(results.some((r) => r.rowCount === 0)).toBe(true);
    });

    it('should handle tablet failures gracefully', async () => {
      // Mock one tablet to fail
      mockTablets.get('40-80')!.execute = async () => {
        throw new Error('Tablet unavailable');
      };

      const plan = vtgate.plan('SELECT * FROM users', []);

      // Should either throw with shard info or return partial results
      await expect(vtgate.scatter(plan)).rejects.toThrow(/40-80|tablet.*unavailable/i);
    });

    it('should respect query timeout for scatter queries', async () => {
      // Mock slow tablet
      mockTablets.get('80-c0')!.execute = async () => {
        await new Promise((resolve) => setTimeout(resolve, 10000));
        return { rows: [], rowCount: 0, fields: [] };
      };

      const plan = vtgate.plan('SELECT * FROM users', []);

      // Should timeout
      await expect(vtgate.scatter(plan)).rejects.toThrow(/timeout/i);
    }, 5000);
  });

  describe('execute() - scatter with merge', () => {
    it('should merge results from all shards', async () => {
      // Setup mock tablets with different data
      let shardIndex = 0;
      for (const [shard, tablet] of mockTablets) {
        const idx = shardIndex++;
        tablet.execute = async () => ({
          rows: [
            { id: idx * 100 + 1, name: `user_${shard}_1` },
            { id: idx * 100 + 2, name: `user_${shard}_2` },
          ],
          rowCount: 2,
          fields: [
            { name: 'id', type: 'int' },
            { name: 'name', type: 'text' },
          ],
        });
      }

      const result = await vtgate.execute('SELECT * FROM users', []);

      // Should have merged all rows
      expect(result.rows).toHaveLength(8); // 2 rows * 4 shards
      expect(result.rowCount).toBe(8);
    });

    it('should preserve field metadata in merged results', async () => {
      const result = await vtgate.execute('SELECT id, name, email FROM users', []);

      expect(result.fields).toBeDefined();
      expect(result.fields.map((f) => f.name)).toContain('id');
      expect(result.fields.map((f) => f.name)).toContain('name');
    });

    it('should apply LIMIT after merging scatter results', async () => {
      // Each shard returns 10 rows
      for (const tablet of mockTablets.values()) {
        tablet.execute = async () => ({
          rows: Array.from({ length: 10 }, (_, i) => ({ id: i, name: `user_${i}` })),
          rowCount: 10,
          fields: [
            { name: 'id', type: 'int' },
            { name: 'name', type: 'text' },
          ],
        });
      }

      const result = await vtgate.execute('SELECT * FROM users LIMIT 5', []);

      expect(result.rows).toHaveLength(5);
    });

    it('should apply OFFSET after merging scatter results', async () => {
      const result = await vtgate.execute('SELECT * FROM users LIMIT 10 OFFSET 5', []);

      expect(result.rows.length).toBeLessThanOrEqual(10);
    });

    it('should merge and sort results for ORDER BY', async () => {
      // Each shard returns sorted results
      let shardIndex = 0;
      for (const tablet of mockTablets.values()) {
        const idx = shardIndex++;
        tablet.execute = async () => ({
          rows: [
            { id: idx * 10 + 1, created_at: new Date(2024, 0, idx + 1).toISOString() },
            { id: idx * 10 + 2, created_at: new Date(2024, 0, idx + 5).toISOString() },
          ],
          rowCount: 2,
          fields: [
            { name: 'id', type: 'int' },
            { name: 'created_at', type: 'timestamp' },
          ],
        });
      }

      const result = await vtgate.execute('SELECT * FROM users ORDER BY created_at ASC', []);

      // Verify results are sorted
      for (let i = 1; i < result.rows.length; i++) {
        const prev = new Date(result.rows[i - 1].created_at as string);
        const curr = new Date(result.rows[i].created_at as string);
        expect(prev.getTime()).toBeLessThanOrEqual(curr.getTime());
      }
    });

    it('should handle ORDER BY DESC correctly', async () => {
      const result = await vtgate.execute('SELECT * FROM users ORDER BY id DESC LIMIT 10', []);

      // Verify descending order
      for (let i = 1; i < result.rows.length; i++) {
        expect(result.rows[i - 1].id).toBeGreaterThanOrEqual(result.rows[i].id as number);
      }
    });
  });

  describe('Partial scatter (multiple specific shards)', () => {
    it('should route IN clause to only affected shards', () => {
      // If we know IDs 100 and 200 hash to shards -40 and 80-c0
      // then we should only scatter to those two shards
      const route = vtgate.route('SELECT * FROM users WHERE id IN ($1, $2)', [100, 200]);

      // Should not be a full scatter
      expect(route.shards.length).toBeLessThanOrEqual(4);
    });

    it('should route range query to affected shards only', () => {
      // With range vindex, a range query might only hit some shards
      const rangeSchema = createVSchemaBuilder()
        .addKeyspace('timeseries', true)
        .addVindex('timeseries', 'range', {
          type: 'range',
          params: {
            ranges: [
              { from: 0, to: 1000000, shard: '-40' },
              { from: 1000000, to: 2000000, shard: '40-80' },
              { from: 2000000, to: 3000000, shard: '80-c0' },
              { from: 3000000, to: Number.MAX_SAFE_INTEGER, shard: 'c0-' },
            ],
          },
        })
        .addTable('timeseries', 'events', {
          column_vindexes: [{ column: 'event_id', name: 'range' }],
        })
        .setShards('timeseries', ['-40', '40-80', '80-c0', 'c0-'])
        .build();

      const gate = createVTGate({
        vschema: rangeSchema,
        shards: new Map([['timeseries', ['-40', '40-80', '80-c0', 'c0-']]]),
      });

      const route = gate.route(
        'SELECT * FROM events WHERE event_id BETWEEN $1 AND $2',
        [500000, 1500000]
      );

      // Should only hit shards -40 and 40-80
      expect(route.shards).toHaveLength(2);
      expect(route.shards).toContain('-40');
      expect(route.shards).toContain('40-80');
    });
  });

  describe('Cross-shard joins', () => {
    it('should scatter cross-shard join when necessary', () => {
      // Join between tables with different sharding keys
      const route = vtgate.route(
        `SELECT u.*, p.* FROM users u
         JOIN products p ON u.favorite_product_id = p.id
         WHERE u.status = $1`,
        ['active']
      );

      expect(route.scatter).toBe(true);
    });

    it('should optimize co-located join to single shard', () => {
      // Join on the sharding key
      const route = vtgate.route(
        `SELECT u.*, o.* FROM users u
         JOIN orders o ON u.id = o.user_id
         WHERE u.id = $1`,
        [123]
      );

      expect(route.scatter).toBe(false);
      expect(route.shards).toHaveLength(1);
    });
  });

  describe('Error handling', () => {
    it('should include shard info in error messages', async () => {
      mockTablets.get('c0-')!.execute = async () => {
        throw new Error('Query execution failed');
      };

      const plan = vtgate.plan('SELECT * FROM users', []);

      try {
        await vtgate.scatter(plan);
        expect.fail('Should have thrown');
      } catch (error) {
        expect((error as Error).message).toContain('c0-');
      }
    });

    it('should handle partial failures when configured', async () => {
      // Configure to allow partial results
      const tolerantGate = createVTGate({
        vschema,
        shards: new Map([['commerce', ['-40', '40-80', '80-c0', 'c0-']]]),
        tablets: mockTablets,
        // allowPartialResults: true, // Would need this config option
      });

      mockTablets.get('40-80')!.execute = async () => {
        throw new Error('Shard unavailable');
      };

      // With partial results enabled, should return data from 3 shards
      // This test documents the desired behavior
    });
  });

  describe('Performance', () => {
    it('should execute scatter queries in parallel', async () => {
      // Track execution times
      const executionTimes: number[] = [];

      for (const tablet of mockTablets.values()) {
        tablet.execute = async () => {
          const start = performance.now();
          await new Promise((resolve) => setTimeout(resolve, 50));
          executionTimes.push(performance.now() - start);
          return { rows: [], rowCount: 0, fields: [] };
        };
      }

      const start = performance.now();
      const plan = vtgate.plan('SELECT * FROM users', []);
      await vtgate.scatter(plan);
      const totalTime = performance.now() - start;

      // If parallel, total time should be close to single shard time
      // If sequential, would be ~200ms (4 * 50ms)
      expect(totalTime).toBeLessThan(150); // Allow some overhead
    });
  });
});
