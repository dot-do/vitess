/**
 * VitessClient Transaction API Tests
 *
 * Issue: vitess-y6r.9
 * TDD Red Phase - Tests define expected behavior before implementation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { VitessClient, createClient, VitessError } from '../client.js';
import type { Transaction } from '../client.js';
import type { ExecuteResult, QueryResult } from '@dotdo/vitess-rpc';

// MessageType constants for test responses
const MessageType = {
  BEGIN: 0x10,
  COMMIT: 0x11,
  ROLLBACK: 0x12,
  QUERY: 0x01,
  EXECUTE: 0x02,
  RESULT: 0x80,
  ERROR: 0x81,
};

interface User {
  id: number;
  name: string;
  balance: number;
}

describe('VitessClient Transaction API', () => {
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

  describe('transaction() basic operations', () => {
    it('should begin a transaction and receive txId', async () => {
      // Mock BEGIN response
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-12345',
          shards: ['shard-01', 'shard-02'],
        }),
      });

      // Mock COMMIT response
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          id: 'commit-1',
          timestamp: Date.now(),
        }),
      });

      let capturedTx: Transaction | undefined;

      await client.transaction(async (tx) => {
        capturedTx = tx;
      });

      expect(capturedTx).toBeDefined();
      expect(capturedTx!.id).toBe('tx-12345');
      expect(capturedTx!.shards).toContain('shard-01');
      expect(capturedTx!.shards).toContain('shard-02');
    });

    it('should auto-commit on successful transaction completion', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-auto-commit',
          shards: ['shard-01'],
        }),
      });

      // Mock execute within transaction
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 } as ExecuteResult,
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await client.transaction(async (tx) => {
        await tx.execute('UPDATE users SET name = $1 WHERE id = $2', ['Alice', 1]);
      });

      // Verify COMMIT was called
      const calls = mockFetch.mock.calls;
      const commitCall = calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.COMMIT;
      });
      expect(commitCall).toBeDefined();
    });

    it('should auto-rollback on error within transaction', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-rollback-on-error',
          shards: ['shard-01'],
        }),
      });

      // Mock ROLLBACK
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await expect(
        client.transaction(async () => {
          throw new Error('Simulated error in transaction');
        })
      ).rejects.toThrow('Simulated error in transaction');

      // Verify ROLLBACK was called
      const calls = mockFetch.mock.calls;
      const rollbackCall = calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.ROLLBACK;
      });
      expect(rollbackCall).toBeDefined();
    });

    it('should return value from transaction function', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-return-value',
          shards: ['shard-01'],
        }),
      });

      // Mock query within transaction
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [{ id: 1, name: 'Alice', balance: 100 }],
            rowCount: 1,
          } as QueryResult<User>,
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      const result = await client.transaction(async (tx) => {
        const res = await tx.query<User>('SELECT * FROM users WHERE id = $1', [1]);
        return res.rows[0];
      });

      expect(result).toEqual({ id: 1, name: 'Alice', balance: 100 });
    });
  });

  describe('transaction() query and execute within transaction', () => {
    it('should execute query() within transaction with txId', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-query-test',
          shards: ['shard-01'],
        }),
      });

      // Mock query
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [{ count: 5 }],
            rowCount: 1,
          },
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await client.transaction(async (tx) => {
        await tx.query('SELECT COUNT(*) as count FROM users');
      });

      // Verify query request included txId
      const queryCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.QUERY;
      });
      expect(queryCall).toBeDefined();
      const queryBody = JSON.parse(queryCall![1].body);
      expect(queryBody.txId).toBe('tx-query-test');
    });

    it('should execute execute() within transaction with txId', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-execute-test',
          shards: ['shard-01'],
        }),
      });

      // Mock execute
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 } as ExecuteResult,
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await client.transaction(async (tx) => {
        await tx.execute('UPDATE users SET name = $1 WHERE id = $2', ['Bob', 1]);
      });

      // Verify execute request included txId
      const executeCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.EXECUTE;
      });
      expect(executeCall).toBeDefined();
      const executeBody = JSON.parse(executeCall![1].body);
      expect(executeBody.txId).toBe('tx-execute-test');
    });

    it('should support multiple operations in a single transaction', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-multi-ops',
          shards: ['shard-01', 'shard-02'],
        }),
      });

      // Mock first execute (debit)
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 } as ExecuteResult,
        }),
      });

      // Mock second execute (credit)
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 } as ExecuteResult,
        }),
      });

      // Mock query (verify)
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: {
            rows: [{ id: 2, balance: 200 }],
            rowCount: 1,
          },
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await client.transaction(async (tx) => {
        // Transfer 100 from account 1 to account 2
        await tx.execute('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, 1]);
        await tx.execute('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, 2]);
        const result = await tx.query('SELECT id, balance FROM accounts WHERE id = $1', [2]);
        expect(result.rows[0].balance).toBe(200);
      });

      // Verify all operations used same txId
      const calls = mockFetch.mock.calls;
      const txCalls = calls.filter((call) => {
        const body = JSON.parse(call[1].body);
        return body.txId === 'tx-multi-ops';
      });
      expect(txCalls.length).toBe(3); // 2 executes + 1 query
    });
  });

  describe('transaction() commit behavior', () => {
    it('should send COMMIT request with txId', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-commit-verify',
          shards: ['shard-01'],
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await client.transaction(async () => {
        // Empty transaction, just testing commit
      });

      const commitCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.COMMIT;
      });
      expect(commitCall).toBeDefined();
      const commitBody = JSON.parse(commitCall![1].body);
      expect(commitBody.txId).toBe('tx-commit-verify');
    });

    it('should throw error if COMMIT fails', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-commit-fail',
          shards: ['shard-01'],
        }),
      });

      // Mock execute (succeeds)
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          result: { affected: 1 },
        }),
      });

      // Mock COMMIT failure
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'COMMIT_FAILED',
          message: 'Failed to commit transaction',
        }),
      });

      await expect(
        client.transaction(async (tx) => {
          await tx.execute('UPDATE users SET name = $1', ['Test']);
        })
      ).rejects.toThrow(VitessError);
    });
  });

  describe('transaction() rollback behavior', () => {
    it('should send ROLLBACK request with txId on error', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-rollback-verify',
          shards: ['shard-01'],
        }),
      });

      // Mock ROLLBACK
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await expect(
        client.transaction(async () => {
          throw new Error('Force rollback');
        })
      ).rejects.toThrow('Force rollback');

      const rollbackCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.ROLLBACK;
      });
      expect(rollbackCall).toBeDefined();
      const rollbackBody = JSON.parse(rollbackCall![1].body);
      expect(rollbackBody.txId).toBe('tx-rollback-verify');
    });

    it('should rollback when VitessError occurs during transaction', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-vitess-error-rollback',
          shards: ['shard-01'],
        }),
      });

      // Mock execute failure (constraint violation)
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'UNIQUE_VIOLATION',
          message: 'Duplicate key',
        }),
      });

      // Mock ROLLBACK
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await expect(
        client.transaction(async (tx) => {
          await tx.execute('INSERT INTO users (email) VALUES ($1)', ['duplicate@example.com']);
        })
      ).rejects.toThrow(VitessError);

      const rollbackCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.ROLLBACK;
      });
      expect(rollbackCall).toBeDefined();
    });

    it('should propagate original error even if rollback fails', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-rollback-fail',
          shards: ['shard-01'],
        }),
      });

      // Mock ROLLBACK failure (connection lost, etc.)
      mockFetch.mockRejectedValueOnce(new Error('Network error during rollback'));

      await expect(
        client.transaction(async () => {
          throw new Error('Original transaction error');
        })
      ).rejects.toThrow('Original transaction error');
    });
  });

  describe('transaction() with options', () => {
    it('should pass isolation level option to BEGIN request', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-isolation',
          shards: ['shard-01'],
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await client.transaction(
        async () => {
          // Empty transaction
        },
        { isolation: 'serializable' }
      );

      const beginCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.BEGIN;
      });
      expect(beginCall).toBeDefined();
      const beginBody = JSON.parse(beginCall![1].body);
      expect(beginBody.options).toBeDefined();
      expect(beginBody.options.isolation).toBe('serializable');
    });

    it('should pass readOnly option to BEGIN request', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-readonly',
          shards: ['shard-01'],
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await client.transaction(
        async () => {
          // Empty transaction
        },
        { readOnly: true }
      );

      const beginCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.BEGIN;
      });
      const beginBody = JSON.parse(beginCall![1].body);
      expect(beginBody.options.readOnly).toBe(true);
    });

    it('should pass timeout option to BEGIN request', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-timeout',
          shards: ['shard-01'],
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await client.transaction(
        async () => {
          // Empty transaction
        },
        { timeout: 5000 }
      );

      const beginCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.BEGIN;
      });
      const beginBody = JSON.parse(beginCall![1].body);
      expect(beginBody.options.timeout).toBe(5000);
    });
  });

  describe('transaction() error scenarios', () => {
    it('should throw VitessError when BEGIN fails', async () => {
      // Mock BEGIN failure
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'BEGIN_FAILED',
          message: 'Cannot begin transaction - server busy',
        }),
      });

      await expect(
        client.transaction(async () => {
          // Should never reach here
        })
      ).rejects.toThrow(VitessError);
    });

    it('should include shard info in error when BEGIN fails on shard', async () => {
      // Mock BEGIN failure with shard info
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'SHARD_UNAVAILABLE',
          message: 'Shard not available',
          shard: 'shard-03',
        }),
      });

      try {
        await client.transaction(async () => {});
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(VitessError);
        expect((error as VitessError).shard).toBe('shard-03');
      }
    });

    it('should handle HTTP errors during transaction', async () => {
      // Mock BEGIN HTTP error
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 503,
        statusText: 'Service Unavailable',
      });

      await expect(
        client.transaction(async () => {})
      ).rejects.toThrow('HTTP 503');
    });
  });

  describe('transaction() request format', () => {
    it('should send BEGIN with correct message format', async () => {
      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-format-test',
          shards: ['shard-01'],
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await client.transaction(async () => {});

      const beginCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.BEGIN;
      });
      expect(beginCall).toBeDefined();

      const beginBody = JSON.parse(beginCall![1].body);
      expect(beginBody.type).toBe(MessageType.BEGIN);
      expect(beginBody.id).toBeDefined();
      expect(typeof beginBody.id).toBe('string');
      expect(beginBody.timestamp).toBeDefined();
      expect(beginBody.keyspace).toBe('main');
    });

    it('should include keyspace in BEGIN request', async () => {
      const customClient = createClient({
        endpoint: 'https://api.vitess.do/v1',
        keyspace: 'custom_keyspace',
      });

      // Mock connection
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({ type: 0x82, id: 'health', timestamp: Date.now() }),
      });
      await customClient.connect();
      mockFetch.mockClear();

      // Mock BEGIN
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-keyspace',
          shards: ['shard-01'],
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      await customClient.transaction(async () => {});

      const beginCall = mockFetch.mock.calls.find((call) => {
        const body = JSON.parse(call[1].body);
        return body.type === MessageType.BEGIN;
      });
      const beginBody = JSON.parse(beginCall![1].body);
      expect(beginBody.keyspace).toBe('custom_keyspace');
    });
  });

  describe('transaction() shards tracking', () => {
    it('should expose shards involved in transaction', async () => {
      // Mock BEGIN with multiple shards
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-shards',
          shards: ['shard-00', 'shard-01', 'shard-02'],
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      let transactionShards: string[] = [];

      await client.transaction(async (tx) => {
        transactionShards = tx.shards;
      });

      expect(transactionShards).toHaveLength(3);
      expect(transactionShards).toContain('shard-00');
      expect(transactionShards).toContain('shard-01');
      expect(transactionShards).toContain('shard-02');
    });

    it('should handle single-shard transaction', async () => {
      // Mock BEGIN with single shard
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-single-shard',
          shards: ['shard-01'],
        }),
      });

      // Mock COMMIT
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      let transactionShards: string[] = [];

      await client.transaction(async (tx) => {
        transactionShards = tx.shards;
      });

      expect(transactionShards).toHaveLength(1);
      expect(transactionShards[0]).toBe('shard-01');
    });
  });
});
