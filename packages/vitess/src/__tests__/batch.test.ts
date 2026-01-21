/**
 * VitessClient Batch Execution API Tests
 *
 * Issue: vitess-y6r.7
 * TDD Red Phase - Tests define expected behavior before implementation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { VitessClient, createClient, VitessError } from '../client.js';
import type { BatchResult, QueryResult } from '@dotdo/vitess-rpc';

// MessageType constants for test responses
const MessageType = {
  BATCH: 0x03,
  RESULT: 0x80,
  ERROR: 0x81,
};

describe('VitessClient Batch Execution API', () => {
  let client: VitessClient;
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    mockFetch = vi.fn();
    vi.stubGlobal('fetch', mockFetch);

    client = createClient({
      endpoint: 'https://api.vitess.do/v1',
      keyspace: 'main',
    });

    // Mock successful connection
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ type: 0x82, id: 'health', timestamp: Date.now() }),
    });
    await client.connect();
    mockFetch.mockClear();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  describe('batch() basic operations', () => {
    it('should execute multiple statements in a single batch', async () => {
      const mockBatchResult: BatchResult = {
        results: [
          { rows: [], rowCount: 0 },
          { rows: [], rowCount: 0 },
          { rows: [], rowCount: 0 },
        ],
        success: true,
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'batch-1',
          timestamp: Date.now(),
          result: mockBatchResult,
        }),
      });

      const result = await client.batch([
        { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Alice'] },
        { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Bob'] },
        { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Charlie'] },
      ]);

      expect(result.success).toBe(true);
      expect(result.results).toHaveLength(3);
    });

    it('should return individual results for each statement', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results: [
              { rows: [{ id: 1, name: 'Alice' }], rowCount: 1 },
              { rows: [{ id: 2, name: 'Bob' }], rowCount: 1 },
            ],
            success: true,
          } as BatchResult,
        }),
      });

      const result = await client.batch([
        { sql: 'SELECT * FROM users WHERE id = $1', params: [1] },
        { sql: 'SELECT * FROM users WHERE id = $1', params: [2] },
      ]);

      expect(result.results[0].rows[0].name).toBe('Alice');
      expect(result.results[1].rows[0].name).toBe('Bob');
    });

    it('should handle empty batch', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results: [],
            success: true,
          } as BatchResult,
        }),
      });

      const result = await client.batch([]);

      expect(result.success).toBe(true);
      expect(result.results).toHaveLength(0);
    });

    it('should handle single statement batch', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results: [{ rows: [], rowCount: 0 }],
            success: true,
          } as BatchResult,
        }),
      });

      const result = await client.batch([
        { sql: 'UPDATE users SET status = $1', params: ['active'] },
      ]);

      expect(result.success).toBe(true);
      expect(result.results).toHaveLength(1);
    });
  });

  describe('batch() with mixed statement types', () => {
    it('should handle mixed INSERT, UPDATE, DELETE statements', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results: [
              { rows: [], rowCount: 0 }, // INSERT result
              { rows: [], rowCount: 0 }, // UPDATE result
              { rows: [], rowCount: 0 }, // DELETE result
            ],
            success: true,
          } as BatchResult,
        }),
      });

      const result = await client.batch([
        { sql: 'INSERT INTO audit_log (action) VALUES ($1)', params: ['start'] },
        { sql: 'UPDATE users SET last_action = $1 WHERE id = $2', params: ['batch', 1] },
        { sql: 'DELETE FROM temp_data WHERE expires < $1', params: [new Date()] },
      ]);

      expect(result.success).toBe(true);
      expect(result.results).toHaveLength(3);
    });

    it('should handle mix of SELECT and write statements', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results: [
              { rows: [{ count: 100 }], rowCount: 1 },
              { rows: [], rowCount: 0 },
              { rows: [{ count: 101 }], rowCount: 1 },
            ],
            success: true,
          } as BatchResult,
        }),
      });

      const result = await client.batch([
        { sql: 'SELECT COUNT(*) as count FROM users' },
        { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['New User'] },
        { sql: 'SELECT COUNT(*) as count FROM users' },
      ]);

      expect(result.results[0].rows[0].count).toBe(100);
      expect(result.results[2].rows[0].count).toBe(101);
    });
  });

  describe('batch() request format', () => {
    it('should send correct message type (BATCH = 0x03)', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { results: [], success: true },
        }),
      });

      await client.batch([{ sql: 'SELECT 1' }]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.type).toBe(MessageType.BATCH);
    });

    it('should include all statements in request', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { results: [], success: true },
        }),
      });

      const statements = [
        { sql: 'INSERT INTO t1 VALUES ($1)', params: [1] },
        { sql: 'INSERT INTO t2 VALUES ($1)', params: [2] },
        { sql: 'INSERT INTO t3 VALUES ($1)', params: [3] },
      ];

      await client.batch(statements);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.statements).toHaveLength(3);
      expect(requestBody.statements[0].sql).toBe('INSERT INTO t1 VALUES ($1)');
      expect(requestBody.statements[0].params).toEqual([1]);
    });

    it('should include keyspace in request', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { results: [], success: true },
        }),
      });

      await client.batch([{ sql: 'SELECT 1' }]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.keyspace).toBe('main');
    });

    it('should handle statements with and without params', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { results: [{ rows: [], rowCount: 0 }, { rows: [], rowCount: 0 }], success: true },
        }),
      });

      await client.batch([
        { sql: 'SELECT NOW()' }, // no params
        { sql: 'SELECT $1', params: [42] }, // with params
      ]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.statements[0].params).toBeUndefined();
      expect(requestBody.statements[1].params).toEqual([42]);
    });
  });

  describe('batch() error handling', () => {
    it('should throw VitessError when batch fails completely', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'BATCH_ERROR',
          message: 'Failed to execute batch',
        }),
      });

      await expect(
        client.batch([
          { sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Test'] },
        ])
      ).rejects.toThrow(VitessError);
    });

    it('should include failure index when partial batch fails', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results: [
              { rows: [], rowCount: 0 },
              { rows: [], rowCount: 0 },
            ],
            success: false,
            failedAt: 2,
            error: 'Constraint violation at statement 2',
          } as BatchResult,
        }),
      });

      const result = await client.batch([
        { sql: 'INSERT INTO users (id, name) VALUES ($1, $2)', params: [1, 'Alice'] },
        { sql: 'INSERT INTO users (id, name) VALUES ($1, $2)', params: [2, 'Bob'] },
        { sql: 'INSERT INTO users (id, name) VALUES ($1, $2)', params: [1, 'Duplicate'] }, // fails
      ]);

      expect(result.success).toBe(false);
      expect(result.failedAt).toBe(2);
      expect(result.error).toContain('Constraint violation');
    });

    it('should handle HTTP error during batch', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      });

      await expect(
        client.batch([{ sql: 'SELECT 1' }])
      ).rejects.toThrow('HTTP 500');
    });

    it('should include shard info when batch fails on specific shard', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'SHARD_BATCH_ERROR',
          message: 'Batch failed on shard',
          shard: 'shard-03',
        }),
      });

      try {
        await client.batch([{ sql: 'INSERT INTO data VALUES ($1)', params: [1] }]);
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(VitessError);
        expect((error as VitessError).shard).toBe('shard-03');
      }
    });
  });

  describe('batch() atomicity behavior', () => {
    it('should indicate partial results when batch partially succeeds', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results: [
              { rows: [], rowCount: 0 },
              { rows: [], rowCount: 0 },
              // Third statement failed, no result
            ],
            success: false,
            failedAt: 2,
            error: 'Deadlock detected',
          } as BatchResult,
        }),
      });

      const result = await client.batch([
        { sql: 'UPDATE accounts SET balance = balance - 100 WHERE id = 1' },
        { sql: 'UPDATE accounts SET balance = balance + 100 WHERE id = 2' },
        { sql: 'UPDATE accounts SET balance = balance - 50 WHERE id = 3' },
      ]);

      expect(result.success).toBe(false);
      expect(result.results).toHaveLength(2); // Only first two completed
      expect(result.failedAt).toBe(2);
    });

    it('should return all results when batch fully succeeds', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results: [
              { rows: [], rowCount: 0 },
              { rows: [], rowCount: 0 },
              { rows: [], rowCount: 0 },
            ],
            success: true,
          } as BatchResult,
        }),
      });

      const result = await client.batch([
        { sql: 'INSERT INTO log VALUES (1)' },
        { sql: 'INSERT INTO log VALUES (2)' },
        { sql: 'INSERT INTO log VALUES (3)' },
      ]);

      expect(result.success).toBe(true);
      expect(result.results).toHaveLength(3);
      expect(result.failedAt).toBeUndefined();
      expect(result.error).toBeUndefined();
    });
  });

  describe('batch() large batches', () => {
    it('should handle large number of statements', async () => {
      const statementCount = 100;
      const statements = Array.from({ length: statementCount }, (_, i) => ({
        sql: 'INSERT INTO items (index) VALUES ($1)',
        params: [i],
      }));

      const results = Array.from({ length: statementCount }, () => ({
        rows: [],
        rowCount: 0,
      }));

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results,
            success: true,
          } as BatchResult,
        }),
      });

      const result = await client.batch(statements);

      expect(result.success).toBe(true);
      expect(result.results).toHaveLength(statementCount);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.statements).toHaveLength(statementCount);
    });
  });

  describe('batch() parameter handling', () => {
    it('should handle various parameter types in batch', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            results: [
              { rows: [], rowCount: 0 },
              { rows: [], rowCount: 0 },
              { rows: [], rowCount: 0 },
            ],
            success: true,
          },
        }),
      });

      await client.batch([
        { sql: 'INSERT INTO t (num) VALUES ($1)', params: [42] },
        { sql: 'INSERT INTO t (str) VALUES ($1)', params: ['hello'] },
        { sql: 'INSERT INTO t (obj) VALUES ($1)', params: [{ key: 'value' }] },
      ]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.statements[0].params).toEqual([42]);
      expect(requestBody.statements[1].params).toEqual(['hello']);
      expect(requestBody.statements[2].params).toEqual([{ key: 'value' }]);
    });

    it('should handle statements with different param counts', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { results: [], success: true },
        }),
      });

      await client.batch([
        { sql: 'SELECT $1', params: [1] },
        { sql: 'SELECT $1, $2, $3', params: [1, 2, 3] },
        { sql: 'SELECT 1' }, // no params
        { sql: 'SELECT $1, $2', params: ['a', 'b'] },
      ]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.statements[0].params).toHaveLength(1);
      expect(requestBody.statements[1].params).toHaveLength(3);
      expect(requestBody.statements[2].params).toBeUndefined();
      expect(requestBody.statements[3].params).toHaveLength(2);
    });
  });
});
