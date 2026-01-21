/**
 * VTGate Aggregation Tests - COUNT, SUM, AVG, MIN, MAX Across Shards
 *
 * TDD Red tests for cross-shard aggregation in VTGate.
 * These tests define the expected behavior for aggregating results
 * from scatter-gather queries.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  VTGate,
  createVTGate,
  type QueryResult,
  type VTTabletStub,
} from '../../server/vtgate.js';
import {
  aggregateCount,
  aggregateSum,
  aggregateAvg,
  aggregateMin,
  aggregateMax,
  mergeResults,
  mergeSorted,
  groupAndAggregate,
  deduplicate,
  applyAggregations,
  createAggregator,
  type AggregationContext,
  type AggregationOp,
} from '../../server/aggregation.js';
import { createVSchemaBuilder, type VSchema } from '../../server/vschema.js';

describe('VTGate Cross-Shard Aggregation', () => {
  let vtgate: VTGate;
  let vschema: VSchema;
  let mockTablets: Map<string, VTTabletStub>;

  beforeEach(() => {
    vschema = createVSchemaBuilder()
      .addKeyspace('analytics', true)
      .addVindex('analytics', 'hash', { type: 'hash' })
      .addTable('analytics', 'events', {
        column_vindexes: [{ column: 'user_id', name: 'hash' }],
      })
      .addTable('analytics', 'orders', {
        column_vindexes: [{ column: 'customer_id', name: 'hash' }],
      })
      .setShards('analytics', ['-40', '40-80', '80-c0', 'c0-'])
      .build();

    mockTablets = new Map();
    const shards = ['-40', '40-80', '80-c0', 'c0-'];
    for (const shard of shards) {
      mockTablets.set(shard, {
        shard,
        execute: async () => ({ rows: [], rowCount: 0, fields: [] }),
      });
    }

    vtgate = createVTGate({
      vschema,
      shards: new Map([['analytics', shards]]),
      tablets: mockTablets,
    });
  });

  describe('COUNT(*) aggregation', () => {
    it('should aggregate COUNT(*) from all shards', async () => {
      // Mock each shard to return different counts
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ count: 100 }],
        rowCount: 1,
        fields: [{ name: 'count', type: 'bigint' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ count: 200 }],
        rowCount: 1,
        fields: [{ name: 'count', type: 'bigint' }],
      });
      mockTablets.get('80-c0')!.execute = async () => ({
        rows: [{ count: 150 }],
        rowCount: 1,
        fields: [{ name: 'count', type: 'bigint' }],
      });
      mockTablets.get('c0-')!.execute = async () => ({
        rows: [{ count: 50 }],
        rowCount: 1,
        fields: [{ name: 'count', type: 'bigint' }],
      });

      const result = await vtgate.execute('SELECT COUNT(*) as count FROM events', []);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].count).toBe(500); // 100 + 200 + 150 + 50
    });

    it('should handle COUNT(*) with WHERE clause', async () => {
      const result = await vtgate.execute(
        "SELECT COUNT(*) FROM events WHERE status = $1",
        ['active']
      );

      expect(result.rows).toHaveLength(1);
      expect(typeof result.rows[0].count).toBe('number');
    });

    it('should handle COUNT(column) excluding nulls', async () => {
      // COUNT(column) should exclude NULL values
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ count: 80 }], // 20 nulls out of 100
        rowCount: 1,
        fields: [{ name: 'count', type: 'bigint' }],
      });

      const result = await vtgate.execute('SELECT COUNT(email) FROM events', []);

      expect(result.rows[0].count).toBeDefined();
    });

    it('should handle COUNT(DISTINCT column)', async () => {
      // Each shard returns distinct values, need to dedupe across shards
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ status: 'active' }, { status: 'pending' }],
        rowCount: 2,
        fields: [{ name: 'status', type: 'text' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ status: 'active' }, { status: 'completed' }],
        rowCount: 2,
        fields: [{ name: 'status', type: 'text' }],
      });
      // ... other shards

      const result = await vtgate.execute('SELECT COUNT(DISTINCT status) FROM events', []);

      // Should count unique values across all shards
      expect(result.rows[0].count).toBeGreaterThanOrEqual(1);
    });
  });

  describe('SUM aggregation', () => {
    it('should aggregate SUM from all shards', async () => {
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ total: 1000.50 }],
        rowCount: 1,
        fields: [{ name: 'total', type: 'numeric' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ total: 2500.75 }],
        rowCount: 1,
        fields: [{ name: 'total', type: 'numeric' }],
      });
      mockTablets.get('80-c0')!.execute = async () => ({
        rows: [{ total: 750.25 }],
        rowCount: 1,
        fields: [{ name: 'total', type: 'numeric' }],
      });
      mockTablets.get('c0-')!.execute = async () => ({
        rows: [{ total: 500.00 }],
        rowCount: 1,
        fields: [{ name: 'total', type: 'numeric' }],
      });

      const result = await vtgate.execute('SELECT SUM(amount) as total FROM orders', []);

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].total).toBeCloseTo(4751.50, 2);
    });

    it('should handle SUM with NULL values', async () => {
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ total: null }],
        rowCount: 1,
        fields: [{ name: 'total', type: 'numeric' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ total: 100 }],
        rowCount: 1,
        fields: [{ name: 'total', type: 'numeric' }],
      });

      const result = await vtgate.execute('SELECT SUM(amount) as total FROM orders', []);

      // NULL should be ignored, result should be 100
      expect(result.rows[0].total).toBe(100);
    });

    it('should return NULL when all values are NULL', async () => {
      for (const tablet of mockTablets.values()) {
        tablet.execute = async () => ({
          rows: [{ total: null }],
          rowCount: 1,
          fields: [{ name: 'total', type: 'numeric' }],
        });
      }

      const result = await vtgate.execute('SELECT SUM(amount) as total FROM orders WHERE 1=0', []);

      expect(result.rows[0].total).toBeNull();
    });

    it('should handle integer overflow in SUM', async () => {
      // Large values that might overflow 32-bit integers
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ total: BigInt('9223372036854775000') }],
        rowCount: 1,
        fields: [{ name: 'total', type: 'bigint' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ total: BigInt('100') }],
        rowCount: 1,
        fields: [{ name: 'total', type: 'bigint' }],
      });

      const result = await vtgate.execute('SELECT SUM(big_value) as total FROM events', []);

      expect(result.rows[0].total).toBe(BigInt('9223372036854775100'));
    });
  });

  describe('AVG aggregation', () => {
    it('should compute weighted average across shards', async () => {
      // For AVG, we need SUM and COUNT from each shard
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ sum: 1000, count: 10 }], // avg = 100
        rowCount: 1,
        fields: [
          { name: 'sum', type: 'numeric' },
          { name: 'count', type: 'bigint' },
        ],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ sum: 500, count: 5 }], // avg = 100
        rowCount: 1,
        fields: [
          { name: 'sum', type: 'numeric' },
          { name: 'count', type: 'bigint' },
        ],
      });
      mockTablets.get('80-c0')!.execute = async () => ({
        rows: [{ sum: 300, count: 3 }], // avg = 100
        rowCount: 1,
        fields: [
          { name: 'sum', type: 'numeric' },
          { name: 'count', type: 'bigint' },
        ],
      });
      mockTablets.get('c0-')!.execute = async () => ({
        rows: [{ sum: 200, count: 2 }], // avg = 100
        rowCount: 1,
        fields: [
          { name: 'sum', type: 'numeric' },
          { name: 'count', type: 'bigint' },
        ],
      });

      const result = await vtgate.execute('SELECT AVG(amount) as avg FROM orders', []);

      // Total sum = 2000, total count = 20, avg = 100
      expect(result.rows[0].avg).toBe(100);
    });

    it('should handle AVG with different row counts per shard', async () => {
      // Shard 1: 2 rows, values 10 and 20 -> sum=30, count=2, avg=15
      // Shard 2: 8 rows, all value 5 -> sum=40, count=8, avg=5
      // Overall: sum=70, count=10, avg=7 (NOT (15+5)/2=10!)
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ sum: 30, count: 2 }],
        rowCount: 1,
        fields: [
          { name: 'sum', type: 'numeric' },
          { name: 'count', type: 'bigint' },
        ],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ sum: 40, count: 8 }],
        rowCount: 1,
        fields: [
          { name: 'sum', type: 'numeric' },
          { name: 'count', type: 'bigint' },
        ],
      });
      mockTablets.get('80-c0')!.execute = async () => ({
        rows: [{ sum: 0, count: 0 }],
        rowCount: 1,
        fields: [
          { name: 'sum', type: 'numeric' },
          { name: 'count', type: 'bigint' },
        ],
      });
      mockTablets.get('c0-')!.execute = async () => ({
        rows: [{ sum: 0, count: 0 }],
        rowCount: 1,
        fields: [
          { name: 'sum', type: 'numeric' },
          { name: 'count', type: 'bigint' },
        ],
      });

      const result = await vtgate.execute('SELECT AVG(value) as avg FROM events', []);

      expect(result.rows[0].avg).toBe(7);
    });

    it('should return NULL for AVG on empty result set', async () => {
      for (const tablet of mockTablets.values()) {
        tablet.execute = async () => ({
          rows: [{ sum: null, count: 0 }],
          rowCount: 1,
          fields: [
            { name: 'sum', type: 'numeric' },
            { name: 'count', type: 'bigint' },
          ],
        });
      }

      const result = await vtgate.execute('SELECT AVG(amount) as avg FROM orders WHERE 1=0', []);

      expect(result.rows[0].avg).toBeNull();
    });
  });

  describe('MIN aggregation', () => {
    it('should find minimum across all shards', async () => {
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ min: 50 }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'numeric' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ min: 25 }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'numeric' }],
      });
      mockTablets.get('80-c0')!.execute = async () => ({
        rows: [{ min: 75 }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'numeric' }],
      });
      mockTablets.get('c0-')!.execute = async () => ({
        rows: [{ min: 100 }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'numeric' }],
      });

      const result = await vtgate.execute('SELECT MIN(price) as min FROM orders', []);

      expect(result.rows[0].min).toBe(25);
    });

    it('should handle MIN with strings (lexicographic)', async () => {
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ min: 'banana' }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'text' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ min: 'apple' }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'text' }],
      });

      const result = await vtgate.execute('SELECT MIN(name) as min FROM events', []);

      expect(result.rows[0].min).toBe('apple');
    });

    it('should handle MIN with dates', async () => {
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ min: new Date('2024-03-15') }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'timestamp' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ min: new Date('2024-01-01') }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'timestamp' }],
      });

      const result = await vtgate.execute('SELECT MIN(created_at) as min FROM events', []);

      expect(new Date(result.rows[0].min as string).toISOString()).toContain('2024-01-01');
    });

    it('should handle MIN with NULL values', async () => {
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ min: null }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'numeric' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ min: 100 }],
        rowCount: 1,
        fields: [{ name: 'min', type: 'numeric' }],
      });

      const result = await vtgate.execute('SELECT MIN(price) as min FROM orders', []);

      // NULL should be ignored
      expect(result.rows[0].min).toBe(100);
    });
  });

  describe('MAX aggregation', () => {
    it('should find maximum across all shards', async () => {
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ max: 50 }],
        rowCount: 1,
        fields: [{ name: 'max', type: 'numeric' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ max: 250 }],
        rowCount: 1,
        fields: [{ name: 'max', type: 'numeric' }],
      });
      mockTablets.get('80-c0')!.execute = async () => ({
        rows: [{ max: 175 }],
        rowCount: 1,
        fields: [{ name: 'max', type: 'numeric' }],
      });
      mockTablets.get('c0-')!.execute = async () => ({
        rows: [{ max: 100 }],
        rowCount: 1,
        fields: [{ name: 'max', type: 'numeric' }],
      });

      const result = await vtgate.execute('SELECT MAX(price) as max FROM orders', []);

      expect(result.rows[0].max).toBe(250);
    });

    it('should handle MAX with strings', async () => {
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ max: 'banana' }],
        rowCount: 1,
        fields: [{ name: 'max', type: 'text' }],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [{ max: 'zebra' }],
        rowCount: 1,
        fields: [{ name: 'max', type: 'text' }],
      });

      const result = await vtgate.execute('SELECT MAX(name) as max FROM events', []);

      expect(result.rows[0].max).toBe('zebra');
    });
  });

  describe('GROUP BY aggregation', () => {
    it('should aggregate with GROUP BY across shards', async () => {
      // Each shard returns grouped results
      mockTablets.get('-40')!.execute = async () => ({
        rows: [
          { status: 'active', count: 50 },
          { status: 'pending', count: 20 },
        ],
        rowCount: 2,
        fields: [
          { name: 'status', type: 'text' },
          { name: 'count', type: 'bigint' },
        ],
      });
      mockTablets.get('40-80')!.execute = async () => ({
        rows: [
          { status: 'active', count: 30 },
          { status: 'completed', count: 40 },
        ],
        rowCount: 2,
        fields: [
          { name: 'status', type: 'text' },
          { name: 'count', type: 'bigint' },
        ],
      });
      mockTablets.get('80-c0')!.execute = async () => ({
        rows: [{ status: 'active', count: 25 }],
        rowCount: 1,
        fields: [
          { name: 'status', type: 'text' },
          { name: 'count', type: 'bigint' },
        ],
      });
      mockTablets.get('c0-')!.execute = async () => ({
        rows: [{ status: 'pending', count: 15 }],
        rowCount: 1,
        fields: [
          { name: 'status', type: 'text' },
          { name: 'count', type: 'bigint' },
        ],
      });

      const result = await vtgate.execute(
        'SELECT status, COUNT(*) as count FROM events GROUP BY status',
        []
      );

      // Should merge groups across shards
      expect(result.rows).toHaveLength(3); // active, pending, completed

      const activeRow = result.rows.find((r) => r.status === 'active');
      expect(activeRow?.count).toBe(105); // 50 + 30 + 25

      const pendingRow = result.rows.find((r) => r.status === 'pending');
      expect(pendingRow?.count).toBe(35); // 20 + 15

      const completedRow = result.rows.find((r) => r.status === 'completed');
      expect(completedRow?.count).toBe(40);
    });

    it('should handle GROUP BY with multiple columns', async () => {
      const result = await vtgate.execute(
        'SELECT region, status, COUNT(*) as count FROM events GROUP BY region, status',
        []
      );

      expect(result.rows).toBeDefined();
      // Each row should have both region and status
      result.rows.forEach((row) => {
        expect(row).toHaveProperty('region');
        expect(row).toHaveProperty('status');
        expect(row).toHaveProperty('count');
      });
    });

    it('should handle GROUP BY with HAVING clause', async () => {
      const result = await vtgate.execute(
        'SELECT status, COUNT(*) as count FROM events GROUP BY status HAVING COUNT(*) > 10',
        []
      );

      // All returned groups should have count > 10
      result.rows.forEach((row) => {
        expect(row.count).toBeGreaterThan(10);
      });
    });

    it('should handle GROUP BY with ORDER BY aggregation', async () => {
      const result = await vtgate.execute(
        'SELECT status, COUNT(*) as count FROM events GROUP BY status ORDER BY count DESC',
        []
      );

      // Results should be sorted by count descending
      for (let i = 1; i < result.rows.length; i++) {
        expect(result.rows[i - 1].count).toBeGreaterThanOrEqual(result.rows[i].count as number);
      }
    });
  });

  describe('Multiple aggregations', () => {
    it('should handle multiple aggregations in single query', async () => {
      mockTablets.get('-40')!.execute = async () => ({
        rows: [{ count: 100, sum: 5000, min: 10, max: 200 }],
        rowCount: 1,
        fields: [
          { name: 'count', type: 'bigint' },
          { name: 'sum', type: 'numeric' },
          { name: 'min', type: 'numeric' },
          { name: 'max', type: 'numeric' },
        ],
      });
      // ... similar for other shards

      const result = await vtgate.execute(
        'SELECT COUNT(*) as count, SUM(amount) as sum, MIN(amount) as min, MAX(amount) as max FROM orders',
        []
      );

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0]).toHaveProperty('count');
      expect(result.rows[0]).toHaveProperty('sum');
      expect(result.rows[0]).toHaveProperty('min');
      expect(result.rows[0]).toHaveProperty('max');
    });
  });

  describe('Aggregation helper functions', () => {
    const mockResults: QueryResult[] = [
      {
        rows: [{ count: 100, sum: 1000, min: 5, max: 50, value: 10 }],
        rowCount: 1,
        fields: [],
      },
      {
        rows: [{ count: 200, sum: 2000, min: 3, max: 60, value: 20 }],
        rowCount: 1,
        fields: [],
      },
      {
        rows: [{ count: 150, sum: 1500, min: 8, max: 45, value: 15 }],
        rowCount: 1,
        fields: [],
      },
    ];

    it('aggregateCount should sum all counts', () => {
      const result = aggregateCount(mockResults, 'count');
      expect(result).toBe(450);
    });

    it('aggregateSum should sum all sums', () => {
      const result = aggregateSum(mockResults, 'sum');
      expect(result).toBe(4500);
    });

    it('aggregateAvg should compute weighted average', () => {
      const result = aggregateAvg(mockResults, 'sum', 'count');
      expect(result).toBe(10); // 4500 / 450
    });

    it('aggregateMin should find global minimum', () => {
      const result = aggregateMin(mockResults, 'min');
      expect(result).toBe(3);
    });

    it('aggregateMax should find global maximum', () => {
      const result = aggregateMax(mockResults, 'max');
      expect(result).toBe(60);
    });
  });

  describe('Merge and sort', () => {
    it('mergeSorted should perform merge sort from multiple sorted lists', () => {
      const results: QueryResult[] = [
        {
          rows: [{ id: 1 }, { id: 5 }, { id: 9 }],
          rowCount: 3,
          fields: [{ name: 'id', type: 'int' }],
        },
        {
          rows: [{ id: 2 }, { id: 4 }, { id: 8 }],
          rowCount: 3,
          fields: [{ name: 'id', type: 'int' }],
        },
        {
          rows: [{ id: 3 }, { id: 6 }, { id: 7 }],
          rowCount: 3,
          fields: [{ name: 'id', type: 'int' }],
        },
      ];

      const merged = mergeSorted(results, [{ column: 'id', direction: 'ASC' }]);

      expect(merged.rows.map((r) => r.id)).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9]);
    });

    it('mergeSorted should handle LIMIT', () => {
      const results: QueryResult[] = [
        {
          rows: [{ id: 1 }, { id: 4 }],
          rowCount: 2,
          fields: [{ name: 'id', type: 'int' }],
        },
        {
          rows: [{ id: 2 }, { id: 5 }],
          rowCount: 2,
          fields: [{ name: 'id', type: 'int' }],
        },
      ];

      const merged = mergeSorted(results, [{ column: 'id', direction: 'ASC' }], 3);

      expect(merged.rows).toHaveLength(3);
      expect(merged.rows.map((r) => r.id)).toEqual([1, 2, 4]);
    });

    it('mergeResults should concatenate rows from all results', () => {
      const results: QueryResult[] = [
        {
          rows: [{ id: 1 }, { id: 2 }],
          rowCount: 2,
          fields: [{ name: 'id', type: 'int' }],
        },
        {
          rows: [{ id: 3 }],
          rowCount: 1,
          fields: [{ name: 'id', type: 'int' }],
        },
      ];

      const merged = mergeResults(results);

      expect(merged.rows).toHaveLength(3);
      expect(merged.rowCount).toBe(3);
    });
  });

  describe('Deduplication', () => {
    it('deduplicate should remove duplicate rows', () => {
      const result: QueryResult = {
        rows: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
          { id: 1, name: 'Alice' },
          { id: 3, name: 'Charlie' },
        ],
        rowCount: 4,
        fields: [
          { name: 'id', type: 'int' },
          { name: 'name', type: 'text' },
        ],
      };

      const deduped = deduplicate(result);

      expect(deduped.rows).toHaveLength(3);
    });

    it('deduplicate should respect specified columns', () => {
      const result: QueryResult = {
        rows: [
          { id: 1, name: 'Alice', dept: 'Eng' },
          { id: 2, name: 'Alice', dept: 'Sales' },
          { id: 3, name: 'Bob', dept: 'Eng' },
        ],
        rowCount: 3,
        fields: [],
      };

      const deduped = deduplicate(result, ['name']);

      // Only unique by name
      expect(deduped.rows).toHaveLength(2);
    });
  });

  describe('Streaming aggregator', () => {
    it('should aggregate rows incrementally', () => {
      const context: AggregationContext = {
        aggregations: [{ function: 'COUNT', column: '*' }],
      };

      const aggregator = createAggregator(context);

      aggregator.addRows([{ id: 1 }, { id: 2 }]);
      aggregator.addRows([{ id: 3 }]);

      const result = aggregator.finalize();

      expect(result.rows[0].count).toBe(3);
    });

    it('should handle streaming GROUP BY', () => {
      const context: AggregationContext = {
        aggregations: [{ function: 'COUNT', column: '*' }],
        groupBy: ['status'],
      };

      const aggregator = createAggregator(context);

      aggregator.addRows([
        { status: 'active', id: 1 },
        { status: 'pending', id: 2 },
      ]);
      aggregator.addRows([
        { status: 'active', id: 3 },
        { status: 'active', id: 4 },
      ]);

      const result = aggregator.finalize();

      const activeGroup = result.rows.find((r) => r.status === 'active');
      expect(activeGroup?.count).toBe(3);
    });
  });
});
