/**
 * VitessClient Execute (Write) API Tests
 *
 * Issue: vitess-y6r.5
 * TDD Red Phase - Tests define expected behavior before implementation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { VitessClient, createClient, VitessError } from '../client.js';
import type { ExecuteResult } from '@dotdo/vitess-rpc';

// MessageType constants for test responses
const MessageType = {
  EXECUTE: 0x02,
  RESULT: 0x80,
  ERROR: 0x81,
};

describe('VitessClient Execute (Write) API', () => {
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

  describe('execute() INSERT operations', () => {
    it('should insert a single row and return affected count', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'exec-1',
          timestamp: Date.now(),
          result: {
            affected: 1,
          } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'INSERT INTO users (name, email) VALUES ($1, $2)',
        ['Alice', 'alice@example.com']
      );

      expect(result.affected).toBe(1);
    });

    it('should return lastInsertId when available', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            affected: 1,
            lastInsertId: 42,
          } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'INSERT INTO users (name) VALUES ($1)',
        ['Bob']
      );

      expect(result.lastInsertId).toBe(42);
    });

    it('should handle multi-row INSERT', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            affected: 3,
          } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'INSERT INTO users (name) VALUES ($1), ($2), ($3)',
        ['Alice', 'Bob', 'Charlie']
      );

      expect(result.affected).toBe(3);
    });

    it('should handle INSERT with RETURNING clause', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            affected: 1,
            lastInsertId: '550e8400-e29b-41d4-a716-446655440000',
          } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'INSERT INTO users (name) VALUES ($1) RETURNING id',
        ['Dave']
      );

      expect(result.lastInsertId).toBe('550e8400-e29b-41d4-a716-446655440000');
    });
  });

  describe('execute() UPDATE operations', () => {
    it('should update rows and return affected count', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            affected: 5,
          } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'UPDATE users SET status = $1 WHERE last_login < $2',
        ['inactive', '2024-01-01']
      );

      expect(result.affected).toBe(5);
    });

    it('should return 0 affected when no rows match', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            affected: 0,
          } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'UPDATE users SET name = $1 WHERE id = $2',
        ['Nobody', -1]
      );

      expect(result.affected).toBe(0);
    });

    it('should handle UPDATE with multiple SET clauses', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'UPDATE users SET name = $1, email = $2, updated_at = $3 WHERE id = $4',
        ['Alice Updated', 'newemail@example.com', new Date(), 1]
      );

      expect(result.affected).toBe(1);
    });
  });

  describe('execute() DELETE operations', () => {
    it('should delete rows and return affected count', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            affected: 10,
          } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'DELETE FROM sessions WHERE expires_at < $1',
        [new Date()]
      );

      expect(result.affected).toBe(10);
    });

    it('should handle DELETE with complex WHERE clause', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 3 } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'DELETE FROM orders WHERE status = $1 AND created_at < $2 AND user_id IN ($3, $4, $5)',
        ['cancelled', '2024-01-01', 1, 2, 3]
      );

      expect(result.affected).toBe(3);
    });
  });

  describe('execute() DDL operations', () => {
    it('should execute CREATE TABLE statement', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 0 } as ExecuteResult,
        }),
      });

      const result = await client.execute(`
        CREATE TABLE IF NOT EXISTS events (
          id SERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          data JSONB,
          created_at TIMESTAMP DEFAULT NOW()
        )
      `);

      expect(result.affected).toBe(0);
    });

    it('should execute ALTER TABLE statement', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 0 } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'ALTER TABLE users ADD COLUMN phone VARCHAR(20)'
      );

      expect(result.affected).toBe(0);
    });

    it('should execute DROP TABLE statement', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 0 } as ExecuteResult,
        }),
      });

      const result = await client.execute('DROP TABLE IF EXISTS temp_table');

      expect(result.affected).toBe(0);
    });

    it('should execute CREATE INDEX statement', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 0 } as ExecuteResult,
        }),
      });

      const result = await client.execute(
        'CREATE INDEX idx_users_email ON users(email)'
      );

      expect(result.affected).toBe(0);
    });
  });

  describe('execute() request format', () => {
    it('should send correct message type (EXECUTE = 0x02)', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 0 },
        }),
      });

      await client.execute('UPDATE users SET status = $1', ['active']);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.type).toBe(MessageType.EXECUTE);
    });

    it('should include keyspace in request', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 0 },
        }),
      });

      await client.execute('DELETE FROM temp');

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.keyspace).toBe('main');
    });

    it('should include parameters in request body', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 },
        }),
      });

      await client.execute('INSERT INTO log (level, message) VALUES ($1, $2)', [
        'INFO',
        'Test message',
      ]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params).toEqual(['INFO', 'Test message']);
    });

    it('should handle execute without parameters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 0 },
        }),
      });

      await client.execute('TRUNCATE TABLE temp_data');

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params).toBeUndefined();
    });
  });

  describe('execute() error handling', () => {
    it('should throw VitessError on constraint violation', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'UNIQUE_VIOLATION',
          message: 'duplicate key value violates unique constraint "users_email_key"',
        }),
      });

      await expect(
        client.execute('INSERT INTO users (email) VALUES ($1)', ['duplicate@example.com'])
      ).rejects.toThrow(VitessError);

      // Reset mock for second assertion
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'UNIQUE_VIOLATION',
          message: 'duplicate key value violates unique constraint "users_email_key"',
        }),
      });

      try {
        await client.execute('INSERT INTO users (email) VALUES ($1)', ['duplicate@example.com']);
      } catch (error) {
        expect((error as VitessError).code).toBe('UNIQUE_VIOLATION');
      }
    });

    it('should throw VitessError on foreign key violation', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'FOREIGN_KEY_VIOLATION',
          message: 'insert or update on table "orders" violates foreign key constraint',
        }),
      });

      await expect(
        client.execute('INSERT INTO orders (user_id) VALUES ($1)', [9999])
      ).rejects.toThrow('foreign key');
    });

    it('should throw VitessError on syntax error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'SYNTAX_ERROR',
          message: 'syntax error at or near "INSRT"',
        }),
      });

      await expect(client.execute('INSRT INTO users VALUES (1)')).rejects.toThrow(
        'syntax error'
      );
    });

    it('should include shard info when write fails on specific shard', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'SHARD_WRITE_ERROR',
          message: 'Failed to write to shard',
          shard: 'shard-02',
        }),
      });

      try {
        await client.execute('INSERT INTO users (name) VALUES ($1)', ['Test']);
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(VitessError);
        expect((error as VitessError).shard).toBe('shard-02');
      }
    });

    it('should throw on HTTP error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 503,
        statusText: 'Service Unavailable',
      });

      await expect(
        client.execute('UPDATE users SET active = true')
      ).rejects.toThrow('HTTP 503');
    });
  });

  describe('execute() parameter type handling', () => {
    it('should handle string parameters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 },
        }),
      });

      await client.execute('INSERT INTO users (name) VALUES ($1)', ['Alice']);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params[0]).toBe('Alice');
    });

    it('should handle number parameters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 },
        }),
      });

      await client.execute('UPDATE accounts SET balance = $1 WHERE id = $2', [
        100.50,
        42,
      ]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params[0]).toBe(100.50);
      expect(requestBody.params[1]).toBe(42);
    });

    it('should handle boolean parameters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 },
        }),
      });

      await client.execute('UPDATE users SET active = $1 WHERE id = $2', [true, 1]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params[0]).toBe(true);
    });

    it('should handle null parameters', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 },
        }),
      });

      await client.execute('UPDATE users SET deleted_at = $1 WHERE id = $2', [
        null,
        1,
      ]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params[0]).toBeNull();
    });

    it('should handle Date parameters', async () => {
      const date = new Date('2024-06-15T10:30:00Z');

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 },
        }),
      });

      await client.execute('UPDATE events SET scheduled_at = $1 WHERE id = $2', [
        date,
        1,
      ]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params[0]).toBe(date.toISOString());
    });

    it('should handle JSON/object parameters', async () => {
      const metadata = { tags: ['urgent', 'review'], priority: 1 };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 },
        }),
      });

      await client.execute('UPDATE items SET metadata = $1 WHERE id = $2', [
        metadata,
        1,
      ]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params[0]).toEqual(metadata);
    });

    it('should handle array parameters', async () => {
      const ids = [1, 2, 3, 4, 5];

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 5 },
        }),
      });

      await client.execute('DELETE FROM notifications WHERE id = ANY($1)', [ids]);

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.params[0]).toEqual(ids);
    });
  });
});
