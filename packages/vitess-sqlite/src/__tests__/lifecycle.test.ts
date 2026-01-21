/**
 * TursoAdapter Lifecycle Tests
 *
 * Tests for initialization, ready state, and cleanup of the Turso/libSQL adapter.
 * Issue: vitess-1bb.1
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { TursoAdapter, type TursoAdapterConfig } from '../index.js';

describe('TursoAdapter Lifecycle', () => {
  let adapter: TursoAdapter;

  afterEach(async () => {
    if (adapter) {
      await adapter.close();
    }
  });

  describe('initialization', () => {
    it('should create an adapter with in-memory database', () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      expect(adapter).toBeInstanceOf(TursoAdapter);
    });

    it('should create an adapter with file-based database URL', () => {
      adapter = new TursoAdapter({ url: 'file:/tmp/test.db' });
      expect(adapter).toBeInstanceOf(TursoAdapter);
    });

    it('should create an adapter with Turso cloud URL', () => {
      adapter = new TursoAdapter({
        url: 'libsql://my-database-org.turso.io',
        authToken: 'test-token',
      });
      expect(adapter).toBeInstanceOf(TursoAdapter);
    });

    it('should accept sync URL for embedded replicas', () => {
      adapter = new TursoAdapter({
        url: 'file:/tmp/local.db',
        syncUrl: 'libsql://my-database-org.turso.io',
        authToken: 'test-token',
      });
      expect(adapter).toBeInstanceOf(TursoAdapter);
    });

    it('should throw error if URL is missing', () => {
      expect(() => {
        // @ts-expect-error - testing missing URL
        adapter = new TursoAdapter({});
      }).toThrow('URL is required');
    });

    it('should throw error if cloud URL is provided without auth token', () => {
      expect(() => {
        adapter = new TursoAdapter({
          url: 'libsql://my-database-org.turso.io',
        });
      }).toThrow('authToken is required for remote databases');
    });
  });

  describe('ready state', () => {
    it('should not be ready before connect is called', () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      expect(adapter.isReady()).toBe(false);
    });

    it('should be ready after connect is called', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      await adapter.connect();
      expect(adapter.isReady()).toBe(true);
    });

    it('should return a promise from connect', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      const result = adapter.connect();
      expect(result).toBeInstanceOf(Promise);
      await result;
    });

    it('should resolve connect with the adapter instance', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      const result = await adapter.connect();
      expect(result).toBe(adapter);
    });

    it('should be idempotent - multiple connect calls should succeed', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      await adapter.connect();
      await adapter.connect();
      expect(adapter.isReady()).toBe(true);
    });

    it('should emit ready event when connected', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      let readyEmitted = false;
      adapter.on('ready', () => {
        readyEmitted = true;
      });
      await adapter.connect();
      expect(readyEmitted).toBe(true);
    });
  });

  describe('close', () => {
    it('should close the connection', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      await adapter.connect();
      await adapter.close();
      expect(adapter.isReady()).toBe(false);
    });

    it('should be idempotent - multiple close calls should succeed', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      await adapter.connect();
      await adapter.close();
      await adapter.close();
      expect(adapter.isReady()).toBe(false);
    });

    it('should allow reconnection after close', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      await adapter.connect();
      await adapter.close();
      await adapter.connect();
      expect(adapter.isReady()).toBe(true);
    });

    it('should emit close event when closed', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      await adapter.connect();
      let closeEmitted = false;
      adapter.on('close', () => {
        closeEmitted = true;
      });
      await adapter.close();
      expect(closeEmitted).toBe(true);
    });

    it('should reject pending queries when closed', async () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      await adapter.connect();
      const queryPromise = adapter.query('SELECT 1');
      await adapter.close();
      await expect(queryPromise).rejects.toThrow('Connection closed');
    });
  });

  describe('engine type', () => {
    it('should report engine type as sqlite', () => {
      adapter = new TursoAdapter({ url: ':memory:' });
      expect(adapter.engineType).toBe('sqlite');
    });
  });

  describe('connection info', () => {
    it('should return connection URL (masked for cloud)', () => {
      adapter = new TursoAdapter({
        url: 'libsql://my-database-org.turso.io',
        authToken: 'test-token',
      });
      const info = adapter.getConnectionInfo();
      expect(info.url).toBe('libsql://my-database-org.turso.io');
      expect(info.authToken).toBeUndefined(); // Should not expose token
    });

    it('should indicate if using embedded replica', () => {
      adapter = new TursoAdapter({
        url: 'file:/tmp/local.db',
        syncUrl: 'libsql://my-database-org.turso.io',
        authToken: 'test-token',
      });
      const info = adapter.getConnectionInfo();
      expect(info.isEmbeddedReplica).toBe(true);
    });
  });
});
