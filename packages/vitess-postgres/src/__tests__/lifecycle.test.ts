/**
 * PGliteAdapter Lifecycle Tests
 *
 * TDD Red tests for adapter initialization, ready state, and cleanup.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { PGliteAdapter, PGliteAdapterError, PGliteErrorCode } from '../index.js';

describe('PGliteAdapter Lifecycle', () => {
  let adapter: PGliteAdapter;

  afterEach(async () => {
    // Cleanup adapter if it exists and is not closed
    if (adapter && !adapter.closed) {
      try {
        await adapter.close();
      } catch {
        // Ignore cleanup errors
      }
    }
  });

  describe('constructor', () => {
    it('should create adapter with default options', () => {
      adapter = new PGliteAdapter();
      expect(adapter).toBeInstanceOf(PGliteAdapter);
    });

    it('should create adapter with custom data directory', () => {
      adapter = new PGliteAdapter({ dataDir: './test-data' });
      expect(adapter).toBeInstanceOf(PGliteAdapter);
    });

    it('should create adapter with debug mode enabled', () => {
      adapter = new PGliteAdapter({ debug: true });
      expect(adapter).toBeInstanceOf(PGliteAdapter);
    });

    it('should create adapter with initial schema', () => {
      adapter = new PGliteAdapter({
        initSchema: 'CREATE TABLE test (id SERIAL PRIMARY KEY)',
      });
      expect(adapter).toBeInstanceOf(PGliteAdapter);
    });
  });

  describe('ready state', () => {
    it('should not be ready before init() is called', () => {
      adapter = new PGliteAdapter();
      expect(adapter.ready).toBe(false);
    });

    it('should not be closed before close() is called', () => {
      adapter = new PGliteAdapter();
      expect(adapter.closed).toBe(false);
    });
  });

  describe('init()', () => {
    it('should initialize the adapter and set ready to true', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();
      expect(adapter.ready).toBe(true);
    });

    it('should be idempotent - multiple init calls should not error', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();
      await adapter.init(); // Second call should be safe
      expect(adapter.ready).toBe(true);
    });

    it('should execute initial schema if provided', async () => {
      adapter = new PGliteAdapter({
        initSchema: `
          CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL
          );
        `,
      });
      await adapter.init();

      // Verify table was created by querying it
      const result = await adapter.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'users'"
      );
      expect(result.rows).toHaveLength(1);
    });

    it('should throw on init with invalid schema', async () => {
      adapter = new PGliteAdapter({
        initSchema: 'CREATE TABLE invalid syntax here!!!',
      });

      await expect(adapter.init()).rejects.toThrow(PGliteAdapterError);
    });

    it('should throw if init called after close', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();
      await adapter.close();

      await expect(adapter.init()).rejects.toThrow(PGliteAdapterError);
      await expect(adapter.init()).rejects.toMatchObject({
        code: PGliteErrorCode.ALREADY_CLOSED,
      });
    });
  });

  describe('waitReady', () => {
    it('should resolve when adapter is ready', async () => {
      adapter = new PGliteAdapter();
      const initPromise = adapter.init();

      await expect(adapter.waitReady).resolves.toBeUndefined();
      await initPromise;
    });

    it('should resolve immediately if already ready', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();

      await expect(adapter.waitReady).resolves.toBeUndefined();
    });

    it('should allow concurrent waitReady calls', async () => {
      adapter = new PGliteAdapter();
      adapter.init(); // Don't await

      const results = await Promise.all([
        adapter.waitReady,
        adapter.waitReady,
        adapter.waitReady,
      ]);

      expect(results).toEqual([undefined, undefined, undefined]);
      expect(adapter.ready).toBe(true);
    });
  });

  describe('close()', () => {
    it('should close the adapter and set closed to true', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();
      await adapter.close();

      expect(adapter.closed).toBe(true);
    });

    it('should set ready to false after close', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();
      expect(adapter.ready).toBe(true);

      await adapter.close();
      expect(adapter.ready).toBe(false);
    });

    it('should be idempotent - multiple close calls should not error', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();
      await adapter.close();
      await adapter.close(); // Second call should be safe

      expect(adapter.closed).toBe(true);
    });

    it('should be safe to call close without init', async () => {
      adapter = new PGliteAdapter();
      await adapter.close(); // Should not throw

      expect(adapter.closed).toBe(true);
    });

    it('should reject pending queries after close', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();
      await adapter.close();

      await expect(adapter.query('SELECT 1')).rejects.toThrow(PGliteAdapterError);
      await expect(adapter.query('SELECT 1')).rejects.toMatchObject({
        code: PGliteErrorCode.ALREADY_CLOSED,
      });
    });

    it('should rollback active transactions on close', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();

      // Start a transaction but don't commit
      const tx = await adapter.begin();
      await tx.execute('CREATE TABLE temp_test (id INT)');

      // Close should rollback the transaction
      await adapter.close();

      // Reopen and verify table does not exist
      adapter = new PGliteAdapter();
      await adapter.init();

      const result = await adapter.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'temp_test'"
      );
      expect(result.rows).toHaveLength(0);
    });
  });

  describe('state transitions', () => {
    it('should follow valid state transitions: created -> ready -> closed', async () => {
      adapter = new PGliteAdapter();

      // Initial state
      expect(adapter.ready).toBe(false);
      expect(adapter.closed).toBe(false);

      // After init
      await adapter.init();
      expect(adapter.ready).toBe(true);
      expect(adapter.closed).toBe(false);

      // After close
      await adapter.close();
      expect(adapter.ready).toBe(false);
      expect(adapter.closed).toBe(true);
    });

    it('should allow skipping init and going directly to closed', async () => {
      adapter = new PGliteAdapter();
      expect(adapter.ready).toBe(false);
      expect(adapter.closed).toBe(false);

      await adapter.close();
      expect(adapter.ready).toBe(false);
      expect(adapter.closed).toBe(true);
    });
  });

  describe('concurrent operations during lifecycle', () => {
    it('should handle concurrent init calls', async () => {
      adapter = new PGliteAdapter();

      const results = await Promise.all([
        adapter.init(),
        adapter.init(),
        adapter.init(),
      ]);

      // All should resolve without error
      expect(results).toHaveLength(3);
      expect(adapter.ready).toBe(true);
    });

    it('should handle query while init is in progress', async () => {
      adapter = new PGliteAdapter();

      // Start init but don't await
      const initPromise = adapter.init();

      // Query should wait for init to complete
      const queryPromise = adapter.query('SELECT 1 as num');

      await initPromise;
      const result = await queryPromise;

      expect(result.rows).toEqual([{ num: 1 }]);
    });

    it('should reject queries started during close', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();

      // Start close but don't await
      const closePromise = adapter.close();

      // Query started during close should be rejected
      const queryPromise = adapter.query('SELECT 1');

      await closePromise;
      await expect(queryPromise).rejects.toThrow();
    });
  });

  describe('memory management', () => {
    it('should release resources on close', async () => {
      adapter = new PGliteAdapter();
      await adapter.init();

      // Create some data
      await adapter.execute('CREATE TABLE mem_test (data TEXT)');
      await adapter.execute("INSERT INTO mem_test VALUES ('test data')");

      await adapter.close();

      // After close, internal db reference should be null
      // This is tested implicitly by the adapter rejecting queries
      await expect(adapter.query('SELECT * FROM mem_test')).rejects.toThrow();
    });

    it('should handle multiple adapter instances', async () => {
      const adapter1 = new PGliteAdapter();
      const adapter2 = new PGliteAdapter();

      await adapter1.init();
      await adapter2.init();

      // Both should work independently
      await adapter1.execute('CREATE TABLE test1 (id INT)');
      await adapter2.execute('CREATE TABLE test2 (id INT)');

      const result1 = await adapter1.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'test1'"
      );
      const result2 = await adapter2.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'test2'"
      );

      expect(result1.rows).toHaveLength(1);
      expect(result2.rows).toHaveLength(1);

      // test1 should not exist in adapter2 and vice versa
      const cross1 = await adapter1.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'test2'"
      );
      const cross2 = await adapter2.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'test1'"
      );

      expect(cross1.rows).toHaveLength(0);
      expect(cross2.rows).toHaveLength(0);

      await adapter1.close();
      await adapter2.close();
    });
  });
});
