/**
 * TursoAdapter Error Handling Tests
 *
 * Tests for error handling, error types, and error messages in the Turso/libSQL adapter.
 * Issue: vitess-1bb.14
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  TursoAdapter,
  TursoError,
  ConnectionError,
  QueryError,
  TransactionError,
  ConstraintError,
  SyntaxError as SqlSyntaxError,
} from '../index.js';

describe('TursoAdapter Error Handling', () => {
  let adapter: TursoAdapter;

  beforeEach(async () => {
    adapter = new TursoAdapter({ url: ':memory:' });
    await adapter.connect();

    // Create test table
    await adapter.execute(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER CHECK(age >= 0)
      )
    `);

    await adapter.execute(
      "INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)"
    );
  });

  afterEach(async () => {
    if (adapter && adapter.isReady()) {
      await adapter.close();
    }
  });

  describe('TursoError base class', () => {
    it('should be an instance of Error', () => {
      const error = new TursoError('Test error');
      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(TursoError);
    });

    it('should have name, message, and code properties', () => {
      const error = new TursoError('Test error', 'TURSO_ERROR');
      expect(error.name).toBe('TursoError');
      expect(error.message).toBe('Test error');
      expect(error.code).toBe('TURSO_ERROR');
    });

    it('should capture stack trace', () => {
      const error = new TursoError('Test error');
      expect(error.stack).toBeDefined();
      expect(error.stack).toContain('TursoError');
    });

    it('should support cause for error chaining', () => {
      const cause = new Error('Original error');
      const error = new TursoError('Wrapped error', 'WRAPPED', { cause });
      expect(error.cause).toBe(cause);
    });
  });

  describe('ConnectionError', () => {
    it('should be thrown when connecting to invalid URL', async () => {
      const badAdapter = new TursoAdapter({
        url: 'libsql://invalid-url-that-does-not-exist.example.com',
        authToken: 'fake-token',
      });

      await expect(badAdapter.connect()).rejects.toThrow(ConnectionError);
    });

    it('should include connection details in error', async () => {
      const badAdapter = new TursoAdapter({
        url: 'libsql://invalid.example.com',
        authToken: 'fake-token',
      });

      try {
        await badAdapter.connect();
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ConnectionError);
        expect((error as ConnectionError).code).toBe('CONNECTION_FAILED');
        expect((error as ConnectionError).url).toContain('invalid.example.com');
      }
    });

    it('should be thrown when querying closed connection', async () => {
      await adapter.close();

      await expect(adapter.query('SELECT 1')).rejects.toThrow(ConnectionError);
    });

    it('should be thrown on authentication failure', async () => {
      const badAdapter = new TursoAdapter({
        url: 'libsql://my-db.turso.io',
        authToken: 'invalid-token',
      });

      await expect(badAdapter.connect()).rejects.toThrow(ConnectionError);
      try {
        await badAdapter.connect();
      } catch (error) {
        expect((error as ConnectionError).code).toMatch(/AUTH|CONNECTION/);
      }
    });
  });

  describe('QueryError', () => {
    it('should be thrown for invalid SQL syntax', async () => {
      await expect(adapter.query('SELEKT * FROM users')).rejects.toThrow(QueryError);
    });

    it('should include the SQL statement in error', async () => {
      const badSql = 'SELEKT * FROM users';
      try {
        await adapter.query(badSql);
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(QueryError);
        expect((error as QueryError).sql).toBe(badSql);
      }
    });

    it('should include parameter info when available', async () => {
      try {
        await adapter.query('SELECT * FROM users WHERE id = ?', ['not-a-number-expecting-id']);
        // This might succeed depending on SQLite type affinity, so we try a different approach
        await adapter.query('SELECT * FROM nonexistent_table WHERE id = ?', [1]);
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(QueryError);
        expect((error as QueryError).params).toBeDefined();
      }
    });

    it('should be thrown when referencing non-existent table', async () => {
      await expect(adapter.query('SELECT * FROM nonexistent_table')).rejects.toThrow(QueryError);
    });

    it('should be thrown when referencing non-existent column', async () => {
      await expect(adapter.query('SELECT nonexistent_column FROM users')).rejects.toThrow(QueryError);
    });
  });

  describe('SqlSyntaxError', () => {
    it('should be a subclass of QueryError', async () => {
      try {
        await adapter.query('SELECT * FORM users'); // typo: FORM instead of FROM
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(QueryError);
        expect(error).toBeInstanceOf(SqlSyntaxError);
      }
    });

    it('should include position/offset information when available', async () => {
      try {
        await adapter.query('SELECT * users'); // missing FROM
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(SqlSyntaxError);
        // Position might be available depending on libSQL error details
        if ((error as SqlSyntaxError).position !== undefined) {
          expect(typeof (error as SqlSyntaxError).position).toBe('number');
        }
      }
    });
  });

  describe('ConstraintError', () => {
    it('should be thrown for UNIQUE constraint violation', async () => {
      await expect(
        adapter.execute(
          "INSERT INTO users (name, email, age) VALUES ('Bob', 'alice@example.com', 25)"
        )
      ).rejects.toThrow(ConstraintError);
    });

    it('should include constraint name when available', async () => {
      try {
        await adapter.execute(
          "INSERT INTO users (name, email, age) VALUES ('Bob', 'alice@example.com', 25)"
        );
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ConstraintError);
        expect((error as ConstraintError).constraintType).toBe('UNIQUE');
      }
    });

    it('should be thrown for NOT NULL constraint violation', async () => {
      await expect(
        adapter.execute("INSERT INTO users (name, email, age) VALUES (NULL, 'test@example.com', 25)")
      ).rejects.toThrow(ConstraintError);

      try {
        await adapter.execute(
          "INSERT INTO users (name, email, age) VALUES (NULL, 'test@example.com', 25)"
        );
      } catch (error) {
        expect((error as ConstraintError).constraintType).toBe('NOT_NULL');
      }
    });

    it('should be thrown for CHECK constraint violation', async () => {
      await expect(
        adapter.execute(
          "INSERT INTO users (name, email, age) VALUES ('Test', 'test@example.com', -1)"
        )
      ).rejects.toThrow(ConstraintError);

      try {
        await adapter.execute(
          "INSERT INTO users (name, email, age) VALUES ('Test', 'test@example.com', -1)"
        );
      } catch (error) {
        expect((error as ConstraintError).constraintType).toBe('CHECK');
      }
    });

    it('should be thrown for FOREIGN KEY constraint violation', async () => {
      await adapter.execute('PRAGMA foreign_keys = ON');
      await adapter.execute(`
        CREATE TABLE orders (
          id INTEGER PRIMARY KEY,
          user_id INTEGER NOT NULL,
          FOREIGN KEY (user_id) REFERENCES users(id)
        )
      `);

      await expect(adapter.execute('INSERT INTO orders (user_id) VALUES (999)')).rejects.toThrow(
        ConstraintError
      );

      try {
        await adapter.execute('INSERT INTO orders (user_id) VALUES (999)');
      } catch (error) {
        expect((error as ConstraintError).constraintType).toBe('FOREIGN_KEY');
      }
    });

    it('should be thrown for PRIMARY KEY constraint violation', async () => {
      await expect(
        adapter.execute(
          "INSERT INTO users (id, name, email, age) VALUES (1, 'Duplicate', 'dup@example.com', 20)"
        )
      ).rejects.toThrow(ConstraintError);

      try {
        await adapter.execute(
          "INSERT INTO users (id, name, email, age) VALUES (1, 'Duplicate', 'dup@example.com', 20)"
        );
      } catch (error) {
        expect((error as ConstraintError).constraintType).toMatch(/PRIMARY_KEY|UNIQUE/);
      }
    });
  });

  describe('TransactionError', () => {
    it('should be thrown when committing non-existent transaction', async () => {
      await expect(adapter.commit('invalid-tx-id')).rejects.toThrow(TransactionError);
    });

    it('should be thrown when rolling back non-existent transaction', async () => {
      await expect(adapter.rollback('invalid-tx-id')).rejects.toThrow(TransactionError);
    });

    it('should be thrown when using expired/closed transaction', async () => {
      const txId = await adapter.begin();
      await adapter.commit(txId);

      await expect(
        adapter.execute('SELECT 1', [], { txId })
      ).rejects.toThrow(TransactionError);
    });

    it('should include transaction ID in error', async () => {
      try {
        await adapter.commit('invalid-tx-123');
      } catch (error) {
        expect(error).toBeInstanceOf(TransactionError);
        expect((error as TransactionError).txId).toBe('invalid-tx-123');
      }
    });

    it('should be thrown on transaction timeout', async () => {
      const txId = await adapter.begin({ timeout: 50 }); // 50ms timeout

      await new Promise((resolve) => setTimeout(resolve, 100));

      await expect(
        adapter.execute('SELECT 1', [], { txId })
      ).rejects.toThrow(TransactionError);

      try {
        await adapter.execute('SELECT 1', [], { txId });
      } catch (error) {
        expect((error as TransactionError).code).toMatch(/TIMEOUT|EXPIRED/);
      }
    });
  });

  describe('error recovery', () => {
    it('should allow queries after non-fatal error', async () => {
      // Trigger an error
      await expect(adapter.query('SELECT * FROM nonexistent')).rejects.toThrow();

      // Should still work after error
      const result = await adapter.query('SELECT * FROM users');
      expect(result.rows).toHaveLength(1);
    });

    it('should allow reconnection after connection error', async () => {
      await adapter.close();

      // Should fail
      await expect(adapter.query('SELECT 1')).rejects.toThrow();

      // Should succeed after reconnect
      await adapter.connect();
      const result = await adapter.query('SELECT 1 as val');
      expect(result.rows[0].val).toBe(1);
    });

    it('should rollback transaction on error within callback', async () => {
      await expect(
        adapter.transaction(async (tx) => {
          await tx.execute("UPDATE users SET age = 100 WHERE email = 'alice@example.com'");
          throw new Error('Simulated error');
        })
      ).rejects.toThrow('Simulated error');

      // Should have rolled back
      const result = await adapter.query("SELECT age FROM users WHERE email = 'alice@example.com'");
      expect(result.rows[0].age).toBe(30); // Original value
    });
  });

  describe('error serialization', () => {
    it('should serialize to JSON properly', async () => {
      try {
        await adapter.query('SELECT * FROM nonexistent');
      } catch (error) {
        const json = JSON.stringify(error);
        const parsed = JSON.parse(json);
        expect(parsed.message).toBeDefined();
        expect(parsed.code).toBeDefined();
      }
    });

    it('should have toJSON method', () => {
      const error = new TursoError('Test', 'TEST_CODE');
      expect(typeof error.toJSON).toBe('function');
      const json = error.toJSON();
      expect(json.message).toBe('Test');
      expect(json.code).toBe('TEST_CODE');
    });
  });

  describe('error events', () => {
    it('should emit error event on query error', async () => {
      let emittedError: Error | null = null;
      adapter.on('error', (err) => {
        emittedError = err;
      });

      await expect(adapter.query('INVALID SQL')).rejects.toThrow();
      expect(emittedError).not.toBeNull();
    });

    it('should emit error event on connection error', async () => {
      const badAdapter = new TursoAdapter({
        url: 'libsql://invalid.example.com',
        authToken: 'fake',
      });

      let emittedError: Error | null = null;
      badAdapter.on('error', (err) => {
        emittedError = err;
      });

      await expect(badAdapter.connect()).rejects.toThrow();
      expect(emittedError).not.toBeNull();
    });
  });

  describe('error codes', () => {
    it('should use consistent error codes', async () => {
      // CONNECTION_FAILED
      const badAdapter = new TursoAdapter({
        url: 'libsql://invalid.example.com',
        authToken: 'fake',
      });
      try {
        await badAdapter.connect();
      } catch (e) {
        expect((e as TursoError).code).toBe('CONNECTION_FAILED');
      }

      // QUERY_ERROR / SYNTAX_ERROR
      try {
        await adapter.query('INVALID');
      } catch (e) {
        expect((e as TursoError).code).toMatch(/QUERY_ERROR|SYNTAX_ERROR/);
      }

      // CONSTRAINT_VIOLATION
      try {
        await adapter.execute(
          "INSERT INTO users (name, email, age) VALUES ('X', 'alice@example.com', 1)"
        );
      } catch (e) {
        expect((e as TursoError).code).toBe('CONSTRAINT_VIOLATION');
      }

      // TRANSACTION_NOT_FOUND
      try {
        await adapter.commit('invalid');
      } catch (e) {
        expect((e as TursoError).code).toBe('TRANSACTION_NOT_FOUND');
      }
    });
  });

  describe('original error preservation', () => {
    it('should preserve original libSQL error as cause', async () => {
      try {
        await adapter.query('SELECT * FROM nonexistent_table');
      } catch (error) {
        expect(error).toBeInstanceOf(TursoError);
        // Original error should be preserved
        expect((error as TursoError).cause).toBeDefined();
      }
    });
  });
});
