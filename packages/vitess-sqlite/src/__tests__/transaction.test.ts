/**
 * TursoAdapter Transaction Support Tests
 *
 * Tests for transaction lifecycle and ACID properties in SQLite via Turso/libSQL.
 * Issue: vitess-1bb.7
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { TursoAdapter } from '../index.js';
import type { TransactionOptions } from '@dotdo/vitess-rpc';

describe('TursoAdapter Transaction Support', () => {
  let adapter: TursoAdapter;

  beforeEach(async () => {
    adapter = new TursoAdapter({ url: ':memory:' });
    await adapter.connect();

    // Create test table
    await adapter.execute(`
      CREATE TABLE accounts (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        balance INTEGER NOT NULL DEFAULT 0
      )
    `);

    // Insert test data
    await adapter.execute(`INSERT INTO accounts (id, name, balance) VALUES (1, 'Alice', 1000)`);
    await adapter.execute(`INSERT INTO accounts (id, name, balance) VALUES (2, 'Bob', 500)`);
  });

  afterEach(async () => {
    await adapter.close();
  });

  describe('basic transaction operations', () => {
    it('should begin a transaction', async () => {
      const txId = await adapter.begin();
      expect(txId).toBeDefined();
      expect(typeof txId).toBe('string');
      await adapter.rollback(txId);
    });

    it('should commit a transaction', async () => {
      const txId = await adapter.begin();
      await adapter.execute('UPDATE accounts SET balance = 900 WHERE id = 1', [], { txId });
      await adapter.commit(txId);

      const result = await adapter.query('SELECT balance FROM accounts WHERE id = 1');
      expect(result.rows[0].balance).toBe(900);
    });

    it('should rollback a transaction', async () => {
      const txId = await adapter.begin();
      await adapter.execute('UPDATE accounts SET balance = 0 WHERE id = 1', [], { txId });
      await adapter.rollback(txId);

      const result = await adapter.query('SELECT balance FROM accounts WHERE id = 1');
      expect(result.rows[0].balance).toBe(1000); // Original value
    });

    it('should isolate uncommitted changes', async () => {
      const txId = await adapter.begin();
      await adapter.execute('UPDATE accounts SET balance = 900 WHERE id = 1', [], { txId });

      // Query outside transaction should see original value
      const result = await adapter.query('SELECT balance FROM accounts WHERE id = 1');
      expect(result.rows[0].balance).toBe(1000);

      await adapter.rollback(txId);
    });
  });

  describe('transaction with callback API', () => {
    it('should execute transaction with callback and auto-commit', async () => {
      await adapter.transaction(async (tx) => {
        await tx.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
        await tx.execute('UPDATE accounts SET balance = balance + 100 WHERE id = 2');
      });

      const result = await adapter.query('SELECT * FROM accounts ORDER BY id');
      expect(result.rows[0].balance).toBe(900);
      expect(result.rows[1].balance).toBe(600);
    });

    it('should auto-rollback on error in callback', async () => {
      await expect(
        adapter.transaction(async (tx) => {
          await tx.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
          throw new Error('Simulated failure');
        })
      ).rejects.toThrow('Simulated failure');

      // Changes should be rolled back
      const result = await adapter.query('SELECT balance FROM accounts WHERE id = 1');
      expect(result.rows[0].balance).toBe(1000);
    });

    it('should return value from callback', async () => {
      const result = await adapter.transaction(async (tx) => {
        const res = await tx.query('SELECT SUM(balance) as total FROM accounts');
        return res.rows[0].total;
      });

      expect(result).toBe(1500);
    });
  });

  describe('nested transactions / savepoints', () => {
    it('should support savepoints within a transaction', async () => {
      const txId = await adapter.begin();

      await adapter.execute('UPDATE accounts SET balance = 900 WHERE id = 1', [], { txId });
      await adapter.savepoint('sp1', { txId });

      await adapter.execute('UPDATE accounts SET balance = 800 WHERE id = 1', [], { txId });
      await adapter.rollbackToSavepoint('sp1', { txId });

      await adapter.commit(txId);

      const result = await adapter.query('SELECT balance FROM accounts WHERE id = 1');
      expect(result.rows[0].balance).toBe(900); // Rolled back to savepoint
    });

    it('should release savepoint', async () => {
      const txId = await adapter.begin();

      await adapter.execute('UPDATE accounts SET balance = 900 WHERE id = 1', [], { txId });
      await adapter.savepoint('sp1', { txId });
      await adapter.releaseSavepoint('sp1', { txId });

      await adapter.commit(txId);

      const result = await adapter.query('SELECT balance FROM accounts WHERE id = 1');
      expect(result.rows[0].balance).toBe(900);
    });
  });

  describe('transaction options', () => {
    it('should support read-only transactions', async () => {
      const txId = await adapter.begin({ readOnly: true });

      // Read should work
      const result = await adapter.query('SELECT * FROM accounts', [], { txId });
      expect(result.rows).toHaveLength(2);

      // Write should fail
      await expect(
        adapter.execute('UPDATE accounts SET balance = 0 WHERE id = 1', [], { txId })
      ).rejects.toThrow();

      await adapter.rollback(txId);
    });

    it('should support transaction timeout', async () => {
      const txId = await adapter.begin({ timeout: 100 }); // 100ms timeout

      // Wait for timeout
      await new Promise((resolve) => setTimeout(resolve, 150));

      // Transaction should have expired
      await expect(
        adapter.execute('UPDATE accounts SET balance = 0 WHERE id = 1', [], { txId })
      ).rejects.toThrow(/timeout|expired/i);
    });

    it('should support IMMEDIATE transaction mode', async () => {
      const txId = await adapter.begin({ mode: 'immediate' });
      await adapter.execute('UPDATE accounts SET balance = 900 WHERE id = 1', [], { txId });
      await adapter.commit(txId);

      const result = await adapter.query('SELECT balance FROM accounts WHERE id = 1');
      expect(result.rows[0].balance).toBe(900);
    });

    it('should support EXCLUSIVE transaction mode', async () => {
      const txId = await adapter.begin({ mode: 'exclusive' });
      await adapter.execute('UPDATE accounts SET balance = 900 WHERE id = 1', [], { txId });
      await adapter.commit(txId);

      const result = await adapter.query('SELECT balance FROM accounts WHERE id = 1');
      expect(result.rows[0].balance).toBe(900);
    });
  });

  describe('concurrent transactions', () => {
    it('should handle multiple concurrent transactions', async () => {
      const tx1 = await adapter.begin();
      const tx2 = await adapter.begin();

      await adapter.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1', [], {
        txId: tx1,
      });
      await adapter.execute('UPDATE accounts SET balance = balance + 50 WHERE id = 2', [], {
        txId: tx2,
      });

      await adapter.commit(tx1);
      await adapter.commit(tx2);

      const result = await adapter.query('SELECT * FROM accounts ORDER BY id');
      expect(result.rows[0].balance).toBe(900);
      expect(result.rows[1].balance).toBe(550);
    });
  });

  describe('batch operations in transaction', () => {
    it('should execute batch within transaction', async () => {
      const txId = await adapter.begin();

      const result = await adapter.batch(
        [
          { sql: 'UPDATE accounts SET balance = balance - 100 WHERE id = 1' },
          { sql: 'UPDATE accounts SET balance = balance + 100 WHERE id = 2' },
        ],
        { txId }
      );

      expect(result.success).toBe(true);
      await adapter.commit(txId);

      const queryResult = await adapter.query('SELECT * FROM accounts ORDER BY id');
      expect(queryResult.rows[0].balance).toBe(900);
      expect(queryResult.rows[1].balance).toBe(600);
    });

    it('should rollback batch on failure', async () => {
      const txId = await adapter.begin();

      await expect(
        adapter.batch(
          [
            { sql: 'UPDATE accounts SET balance = balance - 100 WHERE id = 1' },
            { sql: 'UPDATE nonexistent_table SET foo = 1' }, // This should fail
            { sql: 'UPDATE accounts SET balance = balance + 100 WHERE id = 2' },
          ],
          { txId }
        )
      ).rejects.toThrow();

      await adapter.rollback(txId);

      // All changes should be rolled back
      const result = await adapter.query('SELECT * FROM accounts ORDER BY id');
      expect(result.rows[0].balance).toBe(1000);
      expect(result.rows[1].balance).toBe(500);
    });
  });

  describe('transaction state management', () => {
    it('should throw when committing non-existent transaction', async () => {
      await expect(adapter.commit('non-existent-tx-id')).rejects.toThrow(
        /transaction.*not found|invalid/i
      );
    });

    it('should throw when rolling back non-existent transaction', async () => {
      await expect(adapter.rollback('non-existent-tx-id')).rejects.toThrow(
        /transaction.*not found|invalid/i
      );
    });

    it('should throw when using committed transaction', async () => {
      const txId = await adapter.begin();
      await adapter.commit(txId);

      await expect(
        adapter.execute('UPDATE accounts SET balance = 0 WHERE id = 1', [], { txId })
      ).rejects.toThrow(/transaction.*committed|closed|invalid/i);
    });

    it('should throw when using rolled back transaction', async () => {
      const txId = await adapter.begin();
      await adapter.rollback(txId);

      await expect(
        adapter.execute('UPDATE accounts SET balance = 0 WHERE id = 1', [], { txId })
      ).rejects.toThrow(/transaction.*rolled back|closed|invalid/i);
    });

    it('should list active transactions', () => {
      adapter.begin().then(async (tx1) => {
        const activeTxs = adapter.getActiveTransactions();
        expect(activeTxs).toContain(tx1);
        await adapter.rollback(tx1);
      });
    });
  });

  describe('transaction events', () => {
    it('should emit transaction:begin event', async () => {
      let beginEmitted = false;
      adapter.on('transaction:begin', () => {
        beginEmitted = true;
      });

      const txId = await adapter.begin();
      expect(beginEmitted).toBe(true);
      await adapter.rollback(txId);
    });

    it('should emit transaction:commit event', async () => {
      let commitEmitted = false;
      adapter.on('transaction:commit', () => {
        commitEmitted = true;
      });

      const txId = await adapter.begin();
      await adapter.commit(txId);
      expect(commitEmitted).toBe(true);
    });

    it('should emit transaction:rollback event', async () => {
      let rollbackEmitted = false;
      adapter.on('transaction:rollback', () => {
        rollbackEmitted = true;
      });

      const txId = await adapter.begin();
      await adapter.rollback(txId);
      expect(rollbackEmitted).toBe(true);
    });
  });
});
