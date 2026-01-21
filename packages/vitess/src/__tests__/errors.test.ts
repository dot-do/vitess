/**
 * VitessError Class Tests
 *
 * Issue: vitess-y6r.11
 * TDD Red Phase - Tests define expected behavior before implementation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { VitessClient, createClient, VitessError } from '../client.js';

// MessageType constants for test responses
const MessageType = {
  QUERY: 0x01,
  EXECUTE: 0x02,
  RESULT: 0x80,
  ERROR: 0x81,
};

describe('VitessError Class', () => {
  describe('constructor and properties', () => {
    it('should create error with code and message', () => {
      const error = new VitessError('SYNTAX_ERROR', 'syntax error at position 5');

      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(VitessError);
      expect(error.code).toBe('SYNTAX_ERROR');
      expect(error.message).toBe('syntax error at position 5');
    });

    it('should create error with optional shard info', () => {
      const error = new VitessError('SHARD_ERROR', 'Connection lost', 'shard-03');

      expect(error.code).toBe('SHARD_ERROR');
      expect(error.message).toBe('Connection lost');
      expect(error.shard).toBe('shard-03');
    });

    it('should have undefined shard when not provided', () => {
      const error = new VitessError('GENERIC_ERROR', 'Something went wrong');

      expect(error.shard).toBeUndefined();
    });

    it('should set correct error name', () => {
      const error = new VitessError('TEST_ERROR', 'Test message');

      expect(error.name).toBe('VitessError');
    });

    it('should inherit from Error prototype', () => {
      const error = new VitessError('TEST_ERROR', 'Test message');

      expect(error instanceof Error).toBe(true);
      expect(error.stack).toBeDefined();
    });
  });

  describe('error code constants', () => {
    it('should support SQL syntax error codes', () => {
      const syntaxError = new VitessError('SYNTAX_ERROR', 'Syntax error near SELECT');
      expect(syntaxError.code).toBe('SYNTAX_ERROR');
    });

    it('should support constraint violation codes', () => {
      const uniqueError = new VitessError(
        'UNIQUE_VIOLATION',
        'duplicate key value violates unique constraint'
      );
      expect(uniqueError.code).toBe('UNIQUE_VIOLATION');

      const fkError = new VitessError(
        'FOREIGN_KEY_VIOLATION',
        'violates foreign key constraint'
      );
      expect(fkError.code).toBe('FOREIGN_KEY_VIOLATION');
    });

    it('should support connection error codes', () => {
      const connError = new VitessError(
        'CONNECTION_REFUSED',
        'Could not connect to server'
      );
      expect(connError.code).toBe('CONNECTION_REFUSED');

      const timeoutError = new VitessError('TIMEOUT', 'Query timeout exceeded');
      expect(timeoutError.code).toBe('TIMEOUT');
    });

    it('should support shard-related error codes', () => {
      const shardError = new VitessError(
        'SHARD_UNAVAILABLE',
        'Shard is unavailable',
        'shard-02'
      );
      expect(shardError.code).toBe('SHARD_UNAVAILABLE');
      expect(shardError.shard).toBe('shard-02');
    });

    it('should support transaction error codes', () => {
      const deadlockError = new VitessError(
        'DEADLOCK_DETECTED',
        'Transaction deadlock detected'
      );
      expect(deadlockError.code).toBe('DEADLOCK_DETECTED');

      const rollbackError = new VitessError(
        'TRANSACTION_ROLLBACK',
        'Transaction was rolled back'
      );
      expect(rollbackError.code).toBe('TRANSACTION_ROLLBACK');
    });

    it('should support table/schema error codes', () => {
      const tableError = new VitessError(
        'TABLE_NOT_FOUND',
        'relation "users" does not exist'
      );
      expect(tableError.code).toBe('TABLE_NOT_FOUND');

      const columnError = new VitessError(
        'COLUMN_NOT_FOUND',
        'column "xyz" does not exist'
      );
      expect(columnError.code).toBe('COLUMN_NOT_FOUND');
    });
  });

  describe('error serialization', () => {
    it('should serialize to string correctly', () => {
      const error = new VitessError('TEST_CODE', 'Test message');
      const str = error.toString();

      expect(str).toContain('VitessError');
      expect(str).toContain('Test message');
    });

    it('should have stack trace', () => {
      const error = new VitessError('STACK_TEST', 'Testing stack');

      expect(error.stack).toBeDefined();
      expect(error.stack).toContain('VitessError');
    });

    it('should be JSON serializable', () => {
      const error = new VitessError('JSON_TEST', 'JSON message', 'shard-01');

      const serialized = JSON.stringify({
        name: error.name,
        code: error.code,
        message: error.message,
        shard: error.shard,
      });

      const parsed = JSON.parse(serialized);
      expect(parsed.name).toBe('VitessError');
      expect(parsed.code).toBe('JSON_TEST');
      expect(parsed.message).toBe('JSON message');
      expect(parsed.shard).toBe('shard-01');
    });
  });

  describe('error matching and catching', () => {
    it('should be catchable as VitessError', () => {
      const thrower = () => {
        throw new VitessError('TEST', 'test');
      };

      try {
        thrower();
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
      }
    });

    it('should be catchable as Error', () => {
      const thrower = () => {
        throw new VitessError('TEST', 'test');
      };

      try {
        thrower();
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(Error);
      }
    });

    it('should allow checking error code in catch block', () => {
      try {
        throw new VitessError('UNIQUE_VIOLATION', 'duplicate key');
      } catch (e) {
        if (e instanceof VitessError) {
          expect(e.code).toBe('UNIQUE_VIOLATION');
        } else {
          expect.fail('Should be VitessError');
        }
      }
    });
  });
});

describe('VitessError in Client Operations', () => {
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

  describe('query() error handling', () => {
    it('should throw VitessError with code on query error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'SYNTAX_ERROR',
          message: 'syntax error at or near "SELEC"',
        }),
      });

      try {
        await client.query('SELEC * FROM users');
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).code).toBe('SYNTAX_ERROR');
        expect((e as VitessError).message).toContain('syntax error');
      }
    });

    it('should include shard in VitessError when shard fails', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'SHARD_QUERY_ERROR',
          message: 'Query failed on shard',
          shard: 'shard-02',
        }),
      });

      try {
        await client.query('SELECT * FROM users WHERE tenant_id = $1', [42]);
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).code).toBe('SHARD_QUERY_ERROR');
        expect((e as VitessError).shard).toBe('shard-02');
      }
    });

    it('should throw VitessError with TABLE_NOT_FOUND code', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'TABLE_NOT_FOUND',
          message: 'relation "nonexistent_table" does not exist',
        }),
      });

      try {
        await client.query('SELECT * FROM nonexistent_table');
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).code).toBe('TABLE_NOT_FOUND');
      }
    });
  });

  describe('execute() error handling', () => {
    it('should throw VitessError with UNIQUE_VIOLATION on duplicate', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'UNIQUE_VIOLATION',
          message: 'duplicate key value violates unique constraint "users_email_key"',
        }),
      });

      try {
        await client.execute('INSERT INTO users (email) VALUES ($1)', ['existing@example.com']);
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).code).toBe('UNIQUE_VIOLATION');
      }
    });

    it('should throw VitessError with FOREIGN_KEY_VIOLATION', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'FOREIGN_KEY_VIOLATION',
          message: 'insert violates foreign key constraint "orders_user_id_fkey"',
        }),
      });

      try {
        await client.execute('INSERT INTO orders (user_id) VALUES ($1)', [99999]);
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).code).toBe('FOREIGN_KEY_VIOLATION');
      }
    });

    it('should throw VitessError with NOT_NULL_VIOLATION', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'NOT_NULL_VIOLATION',
          message: 'null value in column "name" violates not-null constraint',
        }),
      });

      try {
        await client.execute('INSERT INTO users (name) VALUES ($1)', [null]);
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).code).toBe('NOT_NULL_VIOLATION');
      }
    });

    it('should throw VitessError with shard info on write failure', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'SHARD_WRITE_ERROR',
          message: 'Write failed on shard',
          shard: 'shard-05',
        }),
      });

      try {
        await client.execute('INSERT INTO data VALUES ($1)', [1]);
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).shard).toBe('shard-05');
      }
    });
  });

  describe('batch() error handling', () => {
    it('should throw VitessError when entire batch fails', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'BATCH_ERROR',
          message: 'Batch execution failed',
        }),
      });

      try {
        await client.batch([
          { sql: 'INSERT INTO t1 VALUES (1)' },
          { sql: 'INSERT INTO t2 VALUES (2)' },
        ]);
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).code).toBe('BATCH_ERROR');
      }
    });
  });

  describe('transaction() error handling', () => {
    it('should throw VitessError when BEGIN fails', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'BEGIN_FAILED',
          message: 'Cannot start transaction',
        }),
      });

      try {
        await client.transaction(async () => {});
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).code).toBe('BEGIN_FAILED');
      }
    });

    it('should throw VitessError with DEADLOCK_DETECTED', async () => {
      // Mock BEGIN success
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          txId: 'tx-deadlock',
          shards: ['shard-01'],
        }),
      });

      // Mock execute with deadlock
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'DEADLOCK_DETECTED',
          message: 'Deadlock detected while waiting for lock',
        }),
      });

      // Mock rollback
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
        }),
      });

      try {
        await client.transaction(async (tx) => {
          await tx.execute('UPDATE accounts SET balance = $1 WHERE id = $2', [100, 1]);
        });
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(VitessError);
        expect((e as VitessError).code).toBe('DEADLOCK_DETECTED');
      }
    });
  });

  describe('HTTP error conversion', () => {
    it('should throw Error (not VitessError) on HTTP errors', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      });

      try {
        await client.query('SELECT 1');
        expect.fail('Should have thrown');
      } catch (e) {
        // HTTP errors are generic Errors, not VitessErrors
        expect(e).toBeInstanceOf(Error);
        expect((e as Error).message).toContain('HTTP 500');
      }
    });

    it('should throw Error on HTTP 503 Service Unavailable', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 503,
        statusText: 'Service Unavailable',
      });

      try {
        await client.execute('UPDATE users SET active = true');
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(Error);
        expect((e as Error).message).toContain('HTTP 503');
      }
    });

    it('should throw Error on HTTP 401 Unauthorized', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 401,
        statusText: 'Unauthorized',
      });

      try {
        await client.query('SELECT * FROM users');
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(Error);
        expect((e as Error).message).toContain('HTTP 401');
      }
    });
  });

  describe('error code enumeration patterns', () => {
    it('should allow switch-case error handling by code', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'UNIQUE_VIOLATION',
          message: 'Duplicate entry',
        }),
      });

      try {
        await client.execute('INSERT INTO users (email) VALUES ($1)', ['test@test.com']);
      } catch (e) {
        if (e instanceof VitessError) {
          switch (e.code) {
            case 'UNIQUE_VIOLATION':
              expect(true).toBe(true); // Expected path
              break;
            case 'FOREIGN_KEY_VIOLATION':
              expect.fail('Wrong error code');
              break;
            default:
              expect.fail('Unknown error code');
          }
        }
      }
    });

    it('should allow pattern matching on multiple error codes', async () => {
      const constraintCodes = ['UNIQUE_VIOLATION', 'FOREIGN_KEY_VIOLATION', 'NOT_NULL_VIOLATION'];

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'FOREIGN_KEY_VIOLATION',
          message: 'FK constraint failed',
        }),
      });

      try {
        await client.execute('DELETE FROM users WHERE id = $1', [1]);
      } catch (e) {
        if (e instanceof VitessError) {
          const isConstraintError = constraintCodes.includes(e.code);
          expect(isConstraintError).toBe(true);
        }
      }
    });
  });
});
