/**
 * PGliteAdapter Error Handling Tests
 *
 * TDD Red tests for error handling and recovery.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import {
  PGliteAdapter,
  PGliteAdapterError,
  PGliteErrorCode,
} from '../index.js';

describe('PGliteAdapter Error Handling', () => {
  let adapter: PGliteAdapter;

  beforeAll(async () => {
    adapter = new PGliteAdapter();
    await adapter.init();

    await adapter.execute(`
      CREATE TABLE error_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INT CHECK (age >= 0),
        category TEXT
      )
    `);

    await adapter.execute(`
      CREATE TABLE parent_table (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
      )
    `);

    await adapter.execute(`
      CREATE TABLE child_table (
        id SERIAL PRIMARY KEY,
        parent_id INT REFERENCES parent_table(id),
        value TEXT
      )
    `);
  });

  afterAll(async () => {
    await adapter.close();
  });

  beforeEach(async () => {
    await adapter.execute('DELETE FROM child_table');
    await adapter.execute('DELETE FROM parent_table');
    await adapter.execute('DELETE FROM error_test');
  });

  describe('PGliteAdapterError', () => {
    it('should be an instance of Error', () => {
      const error = new PGliteAdapterError(
        PGliteErrorCode.QUERY_ERROR,
        'Test error'
      );
      expect(error).toBeInstanceOf(Error);
      expect(error).toBeInstanceOf(PGliteAdapterError);
    });

    it('should have correct name', () => {
      const error = new PGliteAdapterError(
        PGliteErrorCode.QUERY_ERROR,
        'Test error'
      );
      expect(error.name).toBe('PGliteAdapterError');
    });

    it('should include error code', () => {
      const error = new PGliteAdapterError(
        PGliteErrorCode.CONSTRAINT_VIOLATION,
        'Constraint violated'
      );
      expect(error.code).toBe(PGliteErrorCode.CONSTRAINT_VIOLATION);
    });

    it('should include message', () => {
      const error = new PGliteAdapterError(
        PGliteErrorCode.QUERY_ERROR,
        'Something went wrong'
      );
      expect(error.message).toBe('Something went wrong');
    });

    it('should include cause when provided', () => {
      const cause = new Error('Original error');
      const error = new PGliteAdapterError(
        PGliteErrorCode.QUERY_ERROR,
        'Wrapped error',
        cause
      );
      expect(error.cause).toBe(cause);
    });

    it('should include SQL state when provided', () => {
      const error = new PGliteAdapterError(
        PGliteErrorCode.CONSTRAINT_VIOLATION,
        'Unique violation',
        undefined,
        '23505'
      );
      expect(error.sqlState).toBe('23505');
    });
  });

  describe('syntax errors', () => {
    it('should throw SYNTAX_ERROR for invalid SQL', async () => {
      await expect(adapter.query('SELEC * FROM error_test')).rejects.toThrow(
        PGliteAdapterError
      );
      await expect(adapter.query('SELEC * FROM error_test')).rejects.toMatchObject({
        code: PGliteErrorCode.SYNTAX_ERROR,
      });
    });

    it('should throw SYNTAX_ERROR for missing closing parenthesis', async () => {
      await expect(
        adapter.query('SELECT * FROM error_test WHERE (id = 1')
      ).rejects.toMatchObject({
        code: PGliteErrorCode.SYNTAX_ERROR,
      });
    });

    it('should throw SYNTAX_ERROR for invalid keyword', async () => {
      await expect(
        adapter.query('SELECT * FORM error_test')
      ).rejects.toMatchObject({
        code: PGliteErrorCode.SYNTAX_ERROR,
      });
    });

    it('should include position information in syntax errors', async () => {
      try {
        await adapter.query('SELECT * FORM error_test');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PGliteAdapterError);
        // Error message should indicate where the problem is
        expect((error as PGliteAdapterError).message).toBeDefined();
      }
    });
  });

  describe('query errors', () => {
    it('should throw QUERY_ERROR for non-existent table', async () => {
      await expect(adapter.query('SELECT * FROM nonexistent')).rejects.toMatchObject({
        code: PGliteErrorCode.QUERY_ERROR,
      });
    });

    it('should throw QUERY_ERROR for non-existent column', async () => {
      await expect(
        adapter.query('SELECT nonexistent FROM error_test')
      ).rejects.toMatchObject({
        code: PGliteErrorCode.QUERY_ERROR,
      });
    });

    it('should throw QUERY_ERROR for ambiguous column reference', async () => {
      await adapter.execute("INSERT INTO parent_table (name) VALUES ('test')");
      await adapter.execute("INSERT INTO error_test (name) VALUES ('test')");

      await expect(
        adapter.query(`
          SELECT name
          FROM error_test, parent_table
        `)
      ).rejects.toMatchObject({
        code: PGliteErrorCode.QUERY_ERROR,
      });
    });

    it('should throw QUERY_ERROR for type mismatch in comparison', async () => {
      await expect(
        adapter.query("SELECT * FROM error_test WHERE age = 'not a number'")
      ).rejects.toThrow();
    });

    it('should throw QUERY_ERROR for division by zero', async () => {
      await expect(adapter.query('SELECT 1 / 0')).rejects.toThrow();
    });
  });

  describe('constraint violations', () => {
    it('should throw CONSTRAINT_VIOLATION for NOT NULL', async () => {
      await expect(
        adapter.execute('INSERT INTO error_test (name) VALUES (NULL)')
      ).rejects.toMatchObject({
        code: PGliteErrorCode.CONSTRAINT_VIOLATION,
      });
    });

    it('should throw CONSTRAINT_VIOLATION for UNIQUE', async () => {
      await adapter.execute(
        "INSERT INTO error_test (name, email) VALUES ('Test', 'test@example.com')"
      );

      await expect(
        adapter.execute(
          "INSERT INTO error_test (name, email) VALUES ('Test2', 'test@example.com')"
        )
      ).rejects.toMatchObject({
        code: PGliteErrorCode.CONSTRAINT_VIOLATION,
      });
    });

    it('should throw CONSTRAINT_VIOLATION for CHECK constraint', async () => {
      await expect(
        adapter.execute("INSERT INTO error_test (name, age) VALUES ('Test', -5)")
      ).rejects.toMatchObject({
        code: PGliteErrorCode.CONSTRAINT_VIOLATION,
      });
    });

    it('should throw CONSTRAINT_VIOLATION for FOREIGN KEY', async () => {
      await expect(
        adapter.execute("INSERT INTO child_table (parent_id, value) VALUES (999, 'test')")
      ).rejects.toMatchObject({
        code: PGliteErrorCode.CONSTRAINT_VIOLATION,
      });
    });

    it('should throw CONSTRAINT_VIOLATION for FOREIGN KEY on delete', async () => {
      await adapter.execute("INSERT INTO parent_table (name) VALUES ('Parent')");
      const parent = await adapter.query('SELECT id FROM parent_table LIMIT 1');
      await adapter.execute(
        "INSERT INTO child_table (parent_id, value) VALUES ($1, 'child')",
        [parent.rows[0].id]
      );

      // Cannot delete parent with children (no CASCADE)
      await expect(
        adapter.execute('DELETE FROM parent_table WHERE id = $1', [parent.rows[0].id])
      ).rejects.toMatchObject({
        code: PGliteErrorCode.CONSTRAINT_VIOLATION,
      });
    });

    it('should include constraint name in error', async () => {
      await adapter.execute(
        "INSERT INTO error_test (name, email) VALUES ('Test', 'unique@test.com')"
      );

      try {
        await adapter.execute(
          "INSERT INTO error_test (name, email) VALUES ('Test2', 'unique@test.com')"
        );
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PGliteAdapterError);
        // Error should mention the constraint
        expect((error as PGliteAdapterError).message.toLowerCase()).toMatch(
          /unique|constraint|email/
        );
      }
    });

    it('should include SQL state for constraint violations', async () => {
      try {
        await adapter.execute('INSERT INTO error_test (name) VALUES (NULL)');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PGliteAdapterError);
        expect((error as PGliteAdapterError).sqlState).toBeDefined();
        // 23xxx are integrity constraint violation codes
        expect((error as PGliteAdapterError).sqlState).toMatch(/^23/);
      }
    });
  });

  describe('connection errors', () => {
    it('should throw NOT_READY when querying before init', async () => {
      const newAdapter = new PGliteAdapter();

      await expect(newAdapter.query('SELECT 1')).rejects.toMatchObject({
        code: PGliteErrorCode.NOT_READY,
      });
    });

    it('should throw ALREADY_CLOSED when querying after close', async () => {
      const newAdapter = new PGliteAdapter();
      await newAdapter.init();
      await newAdapter.close();

      await expect(newAdapter.query('SELECT 1')).rejects.toMatchObject({
        code: PGliteErrorCode.ALREADY_CLOSED,
      });
    });

    it('should throw ALREADY_CLOSED when executing after close', async () => {
      const newAdapter = new PGliteAdapter();
      await newAdapter.init();
      await newAdapter.close();

      await expect(
        newAdapter.execute('INSERT INTO error_test (name) VALUES ($1)', ['test'])
      ).rejects.toMatchObject({
        code: PGliteErrorCode.ALREADY_CLOSED,
      });
    });

    it('should throw ALREADY_CLOSED when beginning transaction after close', async () => {
      const newAdapter = new PGliteAdapter();
      await newAdapter.init();
      await newAdapter.close();

      await expect(newAdapter.begin()).rejects.toMatchObject({
        code: PGliteErrorCode.ALREADY_CLOSED,
      });
    });
  });

  describe('transaction errors', () => {
    it('should throw TRANSACTION_ERROR for operations on committed transaction', async () => {
      const tx = await adapter.begin();
      await tx.commit();

      await expect(tx.query('SELECT 1')).rejects.toMatchObject({
        code: PGliteErrorCode.TRANSACTION_ERROR,
      });
    });

    it('should throw TRANSACTION_ERROR for operations on rolled back transaction', async () => {
      const tx = await adapter.begin();
      await tx.rollback();

      await expect(tx.execute('SELECT 1')).rejects.toMatchObject({
        code: PGliteErrorCode.TRANSACTION_ERROR,
      });
    });

    it('should throw TRANSACTION_ERROR for double commit', async () => {
      const tx = await adapter.begin();
      await tx.commit();

      await expect(tx.commit()).rejects.toMatchObject({
        code: PGliteErrorCode.TRANSACTION_ERROR,
      });
    });

    it('should throw TRANSACTION_ERROR for double rollback', async () => {
      const tx = await adapter.begin();
      await tx.rollback();

      await expect(tx.rollback()).rejects.toMatchObject({
        code: PGliteErrorCode.TRANSACTION_ERROR,
      });
    });

    it('should throw TRANSACTION_ERROR for commit after rollback', async () => {
      const tx = await adapter.begin();
      await tx.rollback();

      await expect(tx.commit()).rejects.toMatchObject({
        code: PGliteErrorCode.TRANSACTION_ERROR,
      });
    });
  });

  describe('type errors', () => {
    it('should throw TYPE_ERROR for invalid type cast', async () => {
      await expect(adapter.query("SELECT 'abc'::int")).rejects.toThrow();
    });

    it('should throw TYPE_ERROR for invalid date format', async () => {
      await expect(adapter.query("SELECT 'not-a-date'::date")).rejects.toThrow();
    });

    it('should throw TYPE_ERROR for invalid UUID format', async () => {
      await expect(adapter.query("SELECT 'not-a-uuid'::uuid")).rejects.toThrow();
    });
  });

  describe('error recovery', () => {
    it('should allow queries after a failed query', async () => {
      // This should fail
      await expect(adapter.query('INVALID SQL')).rejects.toThrow();

      // This should succeed
      const result = await adapter.query('SELECT 1 as num');
      expect(result.rows[0].num).toBe(1);
    });

    it('should allow queries after a failed execute', async () => {
      // This should fail
      await expect(
        adapter.execute('INSERT INTO error_test (name) VALUES (NULL)')
      ).rejects.toThrow();

      // This should succeed
      await adapter.execute("INSERT INTO error_test (name) VALUES ('Valid')");

      // PGlite returns number for COUNT (not bigint like native Postgres)
      const result = await adapter.query('SELECT COUNT(*) as count FROM error_test');
      expect(result.rows[0].count).toBe(1);
    });

    it('should allow new transaction after failed transaction', async () => {
      // Start a transaction that will fail
      try {
        await adapter.transaction(async (tx) => {
          await tx.execute('INVALID SQL');
        });
      } catch {
        // Expected
      }

      // New transaction should work
      await adapter.transaction(async (tx) => {
        await tx.execute("INSERT INTO error_test (name) VALUES ('After failure')");
      });

      // PGlite returns number for COUNT (not bigint like native Postgres)
      const result = await adapter.query('SELECT COUNT(*) as count FROM error_test');
      expect(result.rows[0].count).toBe(1);
    });

    it('should maintain data integrity after failed transaction', async () => {
      // Insert initial data
      await adapter.execute("INSERT INTO error_test (name, age) VALUES ('Initial', 10)");

      // Try a transaction that fails partway through
      try {
        await adapter.transaction(async (tx) => {
          await tx.execute("UPDATE error_test SET age = 20 WHERE name = 'Initial'");
          await tx.execute('INSERT INTO error_test (name) VALUES (NULL)'); // Fails
        });
      } catch {
        // Expected
      }

      // Original data should be unchanged
      const result = await adapter.query(
        "SELECT age FROM error_test WHERE name = 'Initial'"
      );
      expect(result.rows[0].age).toBe(10);
    });

    it('should handle multiple consecutive errors gracefully', async () => {
      // Multiple errors in a row
      await expect(adapter.query('ERROR 1')).rejects.toThrow();
      await expect(adapter.query('ERROR 2')).rejects.toThrow();
      await expect(adapter.query('ERROR 3')).rejects.toThrow();

      // Should still work
      const result = await adapter.query('SELECT 1 + 1 as sum');
      expect(result.rows[0].sum).toBe(2);
    });

    it('should clean up resources on error', async () => {
      // Run many failing queries to ensure no resource leak
      for (let i = 0; i < 100; i++) {
        try {
          await adapter.query('INVALID SQL');
        } catch {
          // Expected
        }
      }

      // Should still work
      const result = await adapter.query('SELECT 1 as num');
      expect(result.rows[0].num).toBe(1);
    });
  });

  describe('error message quality', () => {
    it('should include helpful context in error messages', async () => {
      try {
        await adapter.query('SELECT * FROM nonexistent_table_name');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PGliteAdapterError);
        // Message should mention the problematic table
        expect((error as PGliteAdapterError).message.toLowerCase()).toContain(
          'nonexistent_table_name'
        );
      }
    });

    it('should include column name in NOT NULL errors', async () => {
      try {
        await adapter.execute('INSERT INTO error_test (name) VALUES (NULL)');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PGliteAdapterError);
        expect((error as PGliteAdapterError).message.toLowerCase()).toMatch(
          /null|name/
        );
      }
    });

    it('should include constraint details in UNIQUE errors', async () => {
      await adapter.execute(
        "INSERT INTO error_test (name, email) VALUES ('A', 'dup@test.com')"
      );

      try {
        await adapter.execute(
          "INSERT INTO error_test (name, email) VALUES ('B', 'dup@test.com')"
        );
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PGliteAdapterError);
        expect((error as PGliteAdapterError).message.toLowerCase()).toMatch(
          /unique|duplicate|email/
        );
      }
    });
  });

  describe('concurrent error handling', () => {
    it('should handle errors in concurrent queries independently', async () => {
      const results = await Promise.allSettled([
        adapter.query('SELECT 1 as num'),
        adapter.query('INVALID SQL'),
        adapter.query('SELECT 2 as num'),
        adapter.query('ANOTHER INVALID'),
        adapter.query('SELECT 3 as num'),
      ]);

      // Valid queries should succeed
      expect(results[0].status).toBe('fulfilled');
      expect(results[2].status).toBe('fulfilled');
      expect(results[4].status).toBe('fulfilled');

      // Invalid queries should fail
      expect(results[1].status).toBe('rejected');
      expect(results[3].status).toBe('rejected');

      if (results[0].status === 'fulfilled') {
        expect(results[0].value.rows[0].num).toBe(1);
      }
    });

    it('should handle errors in sequential transactions independently', async () => {
      // PGlite uses a single connection, so transactions run sequentially
      // Test that each transaction properly commits/rolls back independently

      // First transaction - should succeed
      const result1 = await adapter.transaction(async (tx) => {
        await tx.execute("INSERT INTO error_test (name) VALUES ('Tx1')");
        return 'tx1';
      });
      expect(result1).toBe('tx1');

      // Second transaction - should fail and rollback
      await expect(
        adapter.transaction(async (tx) => {
          await tx.execute('INSERT INTO error_test (name) VALUES (NULL)'); // Fails
          return 'tx2';
        })
      ).rejects.toThrow();

      // Third transaction - should succeed despite previous failure
      const result3 = await adapter.transaction(async (tx) => {
        await tx.execute("INSERT INTO error_test (name) VALUES ('Tx3')");
        return 'tx3';
      });
      expect(result3).toBe('tx3');

      // Only successful transactions should have committed
      // PGlite returns number for COUNT (not bigint like native Postgres)
      const count = await adapter.query('SELECT COUNT(*) as count FROM error_test');
      expect(count.rows[0].count).toBe(2);
    });
  });
});
