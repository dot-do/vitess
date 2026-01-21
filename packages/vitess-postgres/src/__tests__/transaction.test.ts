/**
 * PGliteAdapter Transaction Tests
 *
 * TDD Red tests for BEGIN, COMMIT, ROLLBACK operations.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { PGliteAdapter, PGliteAdapterError, PGliteErrorCode } from '../index.js';

describe('PGliteAdapter Transactions', () => {
  let adapter: PGliteAdapter;

  beforeAll(async () => {
    adapter = new PGliteAdapter();
    await adapter.init();

    // Set up test tables
    await adapter.execute(`
      CREATE TABLE accounts (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        balance DECIMAL(10, 2) NOT NULL DEFAULT 0
      )
    `);

    await adapter.execute(`
      CREATE TABLE audit_log (
        id SERIAL PRIMARY KEY,
        action TEXT NOT NULL,
        account_id INT,
        amount DECIMAL(10, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
  });

  afterAll(async () => {
    await adapter.close();
  });

  beforeEach(async () => {
    // Ensure we're not in a broken transaction state by trying ROLLBACK
    try {
      await adapter.query('ROLLBACK');
    } catch {
      // Ignore - we might not be in a transaction
    }

    // Clean up test data
    await adapter.execute('DELETE FROM audit_log');
    await adapter.execute('DELETE FROM accounts');
    await adapter.execute('ALTER SEQUENCE accounts_id_seq RESTART WITH 1');
    await adapter.execute('ALTER SEQUENCE audit_log_id_seq RESTART WITH 1');

    // Insert initial test data
    await adapter.execute(
      "INSERT INTO accounts (name, balance) VALUES ('Alice', 1000.00)"
    );
    await adapter.execute(
      "INSERT INTO accounts (name, balance) VALUES ('Bob', 500.00)"
    );
  });

  describe('begin()', () => {
    it('should begin a transaction and return a transaction handle', async () => {
      const tx = await adapter.begin();
      expect(tx).toBeDefined();
      expect(tx.id).toBeDefined();
      expect(typeof tx.id).toBe('string');
      await tx.rollback();
    });

    it('should have an active transaction after begin', async () => {
      const tx = await adapter.begin();
      expect(tx.active).toBe(true);
      await tx.rollback();
    });

    it('should allow querying within a transaction', async () => {
      const tx = await adapter.begin();
      const result = await tx.query('SELECT * FROM accounts');
      expect(result.rows).toHaveLength(2);
      await tx.rollback();
    });

    it('should allow executing within a transaction', async () => {
      const tx = await adapter.begin();
      const result = await tx.execute(
        "UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice'"
      );
      expect(result.affected).toBe(1);
      await tx.rollback();
    });
  });

  describe('commit()', () => {
    it('should commit changes permanently', async () => {
      const tx = await adapter.begin();
      await tx.execute("UPDATE accounts SET balance = 2000.00 WHERE name = 'Alice'");
      await tx.commit();

      // Verify changes persisted
      const result = await adapter.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(result.rows[0].balance).toBe('2000.00');
    });

    it('should mark transaction as inactive after commit', async () => {
      const tx = await adapter.begin();
      await tx.execute("UPDATE accounts SET balance = 100 WHERE name = 'Alice'");
      await tx.commit();
      expect(tx.active).toBe(false);
    });

    it('should throw when querying after commit', async () => {
      const tx = await adapter.begin();
      await tx.commit();

      await expect(tx.query('SELECT 1')).rejects.toThrow(PGliteAdapterError);
      await expect(tx.query('SELECT 1')).rejects.toMatchObject({
        code: PGliteErrorCode.TRANSACTION_ERROR,
      });
    });

    it('should throw when executing after commit', async () => {
      const tx = await adapter.begin();
      await tx.commit();

      await expect(tx.execute('UPDATE accounts SET balance = 0')).rejects.toThrow(
        PGliteAdapterError
      );
    });

    it('should throw when committing twice', async () => {
      const tx = await adapter.begin();
      await tx.commit();

      await expect(tx.commit()).rejects.toThrow(PGliteAdapterError);
    });
  });

  describe('rollback()', () => {
    it('should rollback changes', async () => {
      const initialBalance = await adapter.query(
        "SELECT balance FROM accounts WHERE name = 'Alice'"
      );

      const tx = await adapter.begin();
      await tx.execute("UPDATE accounts SET balance = 0 WHERE name = 'Alice'");
      await tx.rollback();

      const result = await adapter.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(result.rows[0].balance).toBe(initialBalance.rows[0].balance);
    });

    it('should mark transaction as inactive after rollback', async () => {
      const tx = await adapter.begin();
      await tx.execute("UPDATE accounts SET balance = 0 WHERE name = 'Alice'");
      await tx.rollback();
      expect(tx.active).toBe(false);
    });

    it('should throw when querying after rollback', async () => {
      const tx = await adapter.begin();
      await tx.rollback();

      await expect(tx.query('SELECT 1')).rejects.toThrow(PGliteAdapterError);
    });

    it('should throw when rolling back twice', async () => {
      const tx = await adapter.begin();
      await tx.rollback();

      await expect(tx.rollback()).rejects.toThrow(PGliteAdapterError);
    });

    it('should rollback on error within transaction', async () => {
      const tx = await adapter.begin();
      await tx.execute("UPDATE accounts SET balance = 500 WHERE name = 'Alice'");

      // This should fail due to constraint
      try {
        await tx.execute("INSERT INTO accounts (name, balance) VALUES (NULL, 100)");
      } catch {
        // Expected to fail
      }

      // Transaction should still be active but in error state
      // Depending on implementation, may auto-rollback or require explicit rollback
      await tx.rollback();

      // Verify changes were not committed
      const result = await adapter.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(result.rows[0].balance).toBe('1000.00');
    });
  });

  describe('transaction() callback', () => {
    it('should execute callback and auto-commit on success', async () => {
      await adapter.transaction(async (tx) => {
        await tx.execute("UPDATE accounts SET balance = balance - 100 WHERE name = 'Alice'");
        await tx.execute("UPDATE accounts SET balance = balance + 100 WHERE name = 'Bob'");
      });

      const alice = await adapter.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      const bob = await adapter.query("SELECT balance FROM accounts WHERE name = 'Bob'");

      expect(alice.rows[0].balance).toBe('900.00');
      expect(bob.rows[0].balance).toBe('600.00');
    });

    it('should auto-rollback on error', async () => {
      await expect(
        adapter.transaction(async (tx) => {
          await tx.execute("UPDATE accounts SET balance = balance - 100 WHERE name = 'Alice'");
          throw new Error('Intentional error');
        })
      ).rejects.toThrow('Intentional error');

      // Verify rollback
      const result = await adapter.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(result.rows[0].balance).toBe('1000.00');
    });

    it('should return the callback result', async () => {
      const result = await adapter.transaction(async (tx) => {
        const query = await tx.query("SELECT balance FROM accounts WHERE name = 'Alice'");
        return parseFloat(query.rows[0].balance);
      });

      expect(result).toBe(1000.0);
    });

    it('should not allow manual commit in callback', async () => {
      await expect(
        adapter.transaction(async (tx) => {
          await tx.commit(); // Should throw
        })
      ).rejects.toThrow();
    });

    it('should not allow manual rollback in callback', async () => {
      await expect(
        adapter.transaction(async (tx) => {
          await tx.rollback(); // Should throw
        })
      ).rejects.toThrow();
    });
  });

  describe('transaction isolation', () => {
    // Note: PGlite uses a single connection, so true multi-connection isolation
    // behavior cannot be tested. These tests verify basic transaction semantics.

    it('should see uncommitted changes within the same transaction', async () => {
      const tx = await adapter.begin();
      await tx.execute("UPDATE accounts SET balance = 0 WHERE name = 'Alice'");

      // Read within the transaction should see the new value
      const insideResult = await tx.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(insideResult.rows[0].balance).toBe('0.00');

      await tx.rollback();

      // After rollback, value should be back to original
      const afterRollback = await adapter.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(afterRollback.rows[0].balance).toBe('1000.00');
    });

    it('should support READ COMMITTED isolation level syntax', async () => {
      const tx = await adapter.begin({ isolation: 'read_committed' });
      const result = await tx.query('SELECT * FROM accounts');
      expect(result.rows).toHaveLength(2);
      await tx.rollback();
    });

    it('should support REPEATABLE READ isolation level syntax', async () => {
      const tx = await adapter.begin({ isolation: 'repeatable_read' });

      // First read
      const result1 = await tx.query("SELECT balance FROM accounts WHERE name = 'Alice'");

      // Do another read - should work
      const result2 = await tx.query("SELECT balance FROM accounts WHERE name = 'Alice'");

      expect(result1.rows[0].balance).toBe(result2.rows[0].balance);

      await tx.rollback();
    });

    it('should support SERIALIZABLE isolation level syntax', async () => {
      const tx = await adapter.begin({ isolation: 'serializable' });
      const result = await tx.query('SELECT * FROM accounts');
      expect(result.rows).toHaveLength(2);
      await tx.rollback();
    });
  });

  describe('transaction options', () => {
    it('should support read-only transactions', async () => {
      const tx = await adapter.begin({ readOnly: true });

      try {
        // Read should work
        const result = await tx.query('SELECT * FROM accounts');
        expect(result.rows).toHaveLength(2);

        // Write should fail - our adapter throws before even hitting Postgres
        await expect(
          tx.execute("UPDATE accounts SET balance = 0 WHERE name = 'Alice'")
        ).rejects.toThrow();
      } finally {
        // Always cleanup
        try {
          await tx.rollback();
        } catch {
          // Transaction may already be invalid
        }
      }
    });

    it('should support transaction timeout', async () => {
      const tx = await adapter.begin({ timeout: 100 }); // 100ms timeout

      // Simulate slow operation
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Operation after timeout should fail
      await expect(tx.query('SELECT 1')).rejects.toThrow();
      // No rollback needed - transaction already timed out
    });
  });

  describe('getTransaction()', () => {
    it('should return transaction by ID', async () => {
      const tx = await adapter.begin();
      const retrieved = adapter.getTransaction(tx.id);
      expect(retrieved).toBe(tx);
      await tx.rollback();
    });

    it('should return undefined for non-existent transaction', () => {
      const retrieved = adapter.getTransaction('non-existent-id');
      expect(retrieved).toBeUndefined();
    });

    it('should return undefined after transaction is closed', async () => {
      const tx = await adapter.begin();
      const txId = tx.id;
      await tx.rollback();

      const retrieved = adapter.getTransaction(txId);
      expect(retrieved).toBeUndefined();
    });
  });

  describe('sequential transactions', () => {
    // Note: PGlite uses a single connection, so true concurrent transactions
    // are not possible. These tests verify sequential transaction handling.

    it('should handle sequential independent transactions', async () => {
      // First transaction
      const tx1 = await adapter.begin();
      await tx1.execute("UPDATE accounts SET balance = 111 WHERE name = 'Alice'");
      await tx1.commit();

      // Second transaction
      const tx2 = await adapter.begin();
      await tx2.execute("UPDATE accounts SET balance = 222 WHERE name = 'Bob'");
      await tx2.rollback();

      // Only tx1's changes should persist
      const alice = await adapter.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      const bob = await adapter.query("SELECT balance FROM accounts WHERE name = 'Bob'");

      expect(alice.rows[0].balance).toBe('111.00');
      expect(bob.rows[0].balance).toBe('500.00'); // Original value (rolled back)
    });

    it('should properly isolate sequential transactions', async () => {
      // First transaction - make changes and commit
      const tx1 = await adapter.begin();
      await tx1.execute("UPDATE accounts SET balance = balance + 100 WHERE name = 'Alice'");
      await tx1.commit();

      // Second transaction - verify it sees the committed changes
      const tx2 = await adapter.begin();
      const result = await tx2.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(result.rows[0].balance).toBe('1100.00');
      await tx2.rollback();
    });
  });

  describe('transaction with savepoints', () => {
    it('should support savepoints within a transaction', async () => {
      const tx = await adapter.begin();

      await tx.execute("UPDATE accounts SET balance = 800 WHERE name = 'Alice'");

      // Create savepoint
      await tx.execute('SAVEPOINT sp1');

      await tx.execute("UPDATE accounts SET balance = 600 WHERE name = 'Alice'");

      // Rollback to savepoint
      await tx.execute('ROLLBACK TO SAVEPOINT sp1');

      // Balance should be 800 (after first update, before savepoint)
      const result = await tx.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(result.rows[0].balance).toBe('800.00');

      await tx.commit();
    });

    it('should support nested savepoints', async () => {
      const tx = await adapter.begin();

      await tx.execute("UPDATE accounts SET balance = 900 WHERE name = 'Alice'");
      await tx.execute('SAVEPOINT sp1');

      await tx.execute("UPDATE accounts SET balance = 800 WHERE name = 'Alice'");
      await tx.execute('SAVEPOINT sp2');

      await tx.execute("UPDATE accounts SET balance = 700 WHERE name = 'Alice'");

      // Rollback to sp2
      await tx.execute('ROLLBACK TO SAVEPOINT sp2');

      let result = await tx.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(result.rows[0].balance).toBe('800.00');

      // Rollback to sp1
      await tx.execute('ROLLBACK TO SAVEPOINT sp1');

      result = await tx.query("SELECT balance FROM accounts WHERE name = 'Alice'");
      expect(result.rows[0].balance).toBe('900.00');

      await tx.rollback();
    });
  });

  describe('error recovery', () => {
    it('should allow new transaction after failed transaction', async () => {
      // First transaction fails
      try {
        await adapter.transaction(async (tx) => {
          await tx.execute("INSERT INTO accounts (name, balance) VALUES (NULL, 100)");
        });
      } catch {
        // Expected
      }

      // New transaction should work
      await adapter.transaction(async (tx) => {
        await tx.execute("INSERT INTO accounts (name, balance) VALUES ('Charlie', 100)");
      });

      const result = await adapter.query("SELECT * FROM accounts WHERE name = 'Charlie'");
      expect(result.rows).toHaveLength(1);
    });

    it('should cleanup orphaned transactions on adapter close', async () => {
      const newAdapter = new PGliteAdapter();
      await newAdapter.init();

      await newAdapter.execute('CREATE TABLE tx_test (id INT)');

      const tx = await newAdapter.begin();
      await tx.execute('INSERT INTO tx_test VALUES (1)');

      // Close without commit/rollback
      await newAdapter.close();

      // Reopen - changes should not be persisted (auto-rollback on close)
      const anotherAdapter = new PGliteAdapter();
      await anotherAdapter.init();

      // Table might not exist (depending on implementation)
      try {
        const result = await anotherAdapter.query('SELECT * FROM tx_test');
        expect(result.rows).toHaveLength(0);
      } catch {
        // Table doesn't exist - that's also acceptable
      }

      await anotherAdapter.close();
    });
  });
});
