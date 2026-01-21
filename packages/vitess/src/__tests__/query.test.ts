/**
 * VitessClient Query Execution API Tests
 *
 * Issue: vitess-y6r.3
 * TDD Red Phase - Tests define expected behavior before implementation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { VitessClient, createClient, VitessError } from '../client.js';
import type { QueryResult, Row } from '@dotdo/vitess-rpc';

// MessageType constants for test responses
const MessageType = {
  QUERY: 0x01,
  RESULT: 0x80,
  ERROR: 0x81,
};

interface User extends Row {
  id: number;
  name: string;
  email: string;
}

interface Order extends Row {
  id: number;
  user_id: number;
  total: number;
  status: string;
}

describe('VitessClient Query Execution API', () => {
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

  describe('query() basic operations', () => {
    it('should execute a simple SELECT query and return results', async () => {
      const mockResult: QueryResult<User> = {
        rows: [
          { id: 1, name: 'Alice', email: 'alice@example.com' },
          { id: 2, name: 'Bob', email: 'bob@example.com' },
        ],
        rowCount: 2,
        fields: [
          { name: 'id', type: 'integer' },
          { name: 'name', type: 'text' },
          { name: 'email', type: 'text' },
        ],
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'query-1',
          timestamp: Date.now(),
          result: mockResult,
        }),
      });

      const result = await client.query<User>('SELECT * FROM users');

      expect(result.rows).toHaveLength(2);
      expect(result.rows[0].name).toBe('Alice');
      expect(result.rowCount).toBe(2);
    });

    it('should return empty result set for queries with no matching rows', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'query-2',
          timestamp: Date.now(),
          result: {
            rows: [],
            rowCount: 0,
            fields: [
              { name: 'id', type: 'integer' },
              { name: 'name', type: 'text' },
            ],
          },
        }),
      });

      const result = await client.query('SELECT * FROM users WHERE id = -1');

      expect(result.rows).toHaveLength(0);
      expect(result.rowCount).toBe(0);
    });

    it('should include field metadata in results', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'query-3',
          timestamp: Date.now(),
          result: {
            rows: [],
            rowCount: 0,
            fields: [
              { name: 'id', type: 'bigint', nativeType: 20 },
              { name: 'created_at', type: 'timestamp', nativeType: 1114 },
            ],
          },
        }),
      });

      const result = await client.query('SELECT id, created_at FROM users');

      expect(result.fields).toBeDefined();
      expect(result.fields).toHaveLength(2);
      expect(result.fields![0].name).toBe('id');
      expect(result.fields![1].type).toBe('timestamp');
    });
  });

  describe('query() with parameters', () => {
    it('should execute query with positional parameters ($1, $2)', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'query-4',
          timestamp: Date.now(),
          result: {
            rows: [{ id: 1, name: 'Alice', email: 'alice@example.com' }],
            rowCount: 1,
          },
        }),
      });

      const result = await client.query<User>(
        'SELECT * FROM users WHERE id = $1 AND status = $2',
        [1, 'active']
      );

      expect(result.rows).toHaveLength(1);

      // Verify the request body contains params
      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params).toEqual([1, 'active']);
    });

    it('should handle null parameter values', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'query-5',
          timestamp: Date.now(),
          result: { rows: [], rowCount: 0 },
        }),
      });

      await client.query('SELECT * FROM users WHERE deleted_at = $1', [null]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params).toEqual([null]);
    });

    it('should handle various parameter types', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'query-6',
          timestamp: Date.now(),
          result: { rows: [], rowCount: 0 },
        }),
      });

      const params = [
        42, // number
        'text', // string
        true, // boolean
        new Date('2024-01-01'), // Date
        { key: 'value' }, // object (JSON)
        [1, 2, 3], // array
      ];

      await client.query('SELECT * FROM mixed_types WHERE data = $1', params);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params).toHaveLength(6);
    });

    it('should handle query without parameters (undefined)', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'query-7',
          timestamp: Date.now(),
          result: { rows: [{ count: 100 }], rowCount: 1 },
        }),
      });

      await client.query('SELECT COUNT(*) as count FROM users');

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params).toBeUndefined();
    });

    it('should handle empty params array', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'query-8',
          timestamp: Date.now(),
          result: { rows: [], rowCount: 0 },
        }),
      });

      await client.query('SELECT * FROM users', []);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params).toEqual([]);
    });
  });

  describe('query() request format', () => {
    it('should send correct message type (QUERY = 0x01)', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { rows: [], rowCount: 0 },
        }),
      });

      await client.query('SELECT 1');

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.type).toBe(MessageType.QUERY);
    });

    it('should include keyspace in request when configured', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { rows: [], rowCount: 0 },
        }),
      });

      await client.query('SELECT 1');

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.keyspace).toBe('main');
    });

    it('should include unique message ID', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { rows: [], rowCount: 0 },
        }),
      });

      await client.query('SELECT 1');

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.id).toBeDefined();
      expect(typeof requestBody.id).toBe('string');
      expect(requestBody.id.length).toBeGreaterThan(0);
    });

    it('should include timestamp', async () => {
      const beforeTime = Date.now();

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { rows: [], rowCount: 0 },
        }),
      });

      await client.query('SELECT 1');

      const afterTime = Date.now();
      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);

      expect(requestBody.timestamp).toBeGreaterThanOrEqual(beforeTime);
      expect(requestBody.timestamp).toBeLessThanOrEqual(afterTime);
    });
  });

  describe('query() error handling', () => {
    it('should throw VitessError on server error response', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          id: 'query-err-1',
          timestamp: Date.now(),
          code: 'SYNTAX_ERROR',
          message: 'syntax error at or near "SELEC"',
        }),
      });

      await expect(client.query('SELEC * FROM users')).rejects.toThrow(VitessError);
      await expect(client.query('SELEC * FROM users')).rejects.toThrow('syntax error');
    });

    it('should include error code in VitessError', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'TABLE_NOT_FOUND',
          message: 'relation "nonexistent" does not exist',
        }),
      });

      try {
        await client.query('SELECT * FROM nonexistent');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(VitessError);
        expect((error as VitessError).code).toBe('TABLE_NOT_FOUND');
      }
    });

    it('should include shard info in VitessError when available', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'SHARD_ERROR',
          message: 'Shard unavailable',
          shard: 'shard-01',
        }),
      });

      try {
        await client.query('SELECT * FROM users WHERE tenant_id = 1');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(VitessError);
        expect((error as VitessError).shard).toBe('shard-01');
      }
    });

    it('should throw on HTTP error response', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      });

      await expect(client.query('SELECT 1')).rejects.toThrow('HTTP 500');
    });
  });

  describe('query() typed results', () => {
    it('should support generic type parameter for type-safe results', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [{ id: 1, user_id: 10, total: 99.99, status: 'pending' }],
            rowCount: 1,
          },
        }),
      });

      const result = await client.query<Order>('SELECT * FROM orders WHERE id = $1', [1]);

      // Type system should know result.rows[0] is Order
      const order = result.rows[0];
      expect(order.id).toBe(1);
      expect(order.total).toBe(99.99);
      expect(order.status).toBe('pending');
    });

    it('should work with default Row type when no generic specified', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [{ arbitrary: 'data', value: 123 }],
            rowCount: 1,
          },
        }),
      });

      const result = await client.query('SELECT * FROM some_table');

      // Default Row type is Record<string, unknown>
      expect(result.rows[0].arbitrary).toBe('data');
    });
  });

  describe('query() duration tracking', () => {
    it('should include query duration when server provides it', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [],
            rowCount: 0,
            duration: 15.5,
          },
        }),
      });

      const result = await client.query('SELECT 1');

      expect(result.duration).toBe(15.5);
    });

    it('should handle missing duration gracefully', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [],
            rowCount: 0,
          },
        }),
      });

      const result = await client.query('SELECT 1');

      expect(result.duration).toBeUndefined();
    });
  });

  describe('query() complex queries', () => {
    it('should handle JOIN queries across tables', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [
              { user_name: 'Alice', order_total: 150 },
              { user_name: 'Bob', order_total: 200 },
            ],
            rowCount: 2,
          },
        }),
      });

      const result = await client.query(
        'SELECT u.name as user_name, o.total as order_total FROM users u JOIN orders o ON u.id = o.user_id'
      );

      expect(result.rows).toHaveLength(2);
      expect(result.rows[0].user_name).toBe('Alice');
    });

    it('should handle aggregate queries', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [{ total_orders: 100, total_amount: 5000.50 }],
            rowCount: 1,
          },
        }),
      });

      const result = await client.query(
        'SELECT COUNT(*) as total_orders, SUM(total) as total_amount FROM orders'
      );

      expect(result.rows[0].total_orders).toBe(100);
      expect(result.rows[0].total_amount).toBe(5000.50);
    });

    it('should handle GROUP BY queries', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [
              { status: 'pending', count: 10 },
              { status: 'completed', count: 50 },
              { status: 'cancelled', count: 5 },
            ],
            rowCount: 3,
          },
        }),
      });

      const result = await client.query(
        'SELECT status, COUNT(*) as count FROM orders GROUP BY status'
      );

      expect(result.rows).toHaveLength(3);
    });
  });
});
