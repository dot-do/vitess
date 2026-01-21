/**
 * VitessClient Status and VSchema API Tests
 *
 * Issue: vitess-y6r.15
 * TDD Red Phase - Tests define expected behavior before implementation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { VitessClient, createClient, VitessError } from '../client.js';
import type { ClusterStatus, ShardHealth, VSchema } from '@dotdo/vitess-rpc';

// MessageType constants for test responses
const MessageType = {
  STATUS: 0x20,
  VSCHEMA: 0x23,
  RESULT: 0x80,
  ERROR: 0x81,
};

describe('VitessClient Status API', () => {
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

  describe('status() method', () => {
    it('should return cluster status with all shards healthy', async () => {
      const mockStatus: ClusterStatus = {
        keyspace: 'main',
        shardCount: 4,
        engine: 'postgres',
        shards: [
          { id: 'shard-00', healthy: true, engine: 'postgres', queryCount: 1000, errorCount: 0, lastQuery: Date.now() },
          { id: 'shard-01', healthy: true, engine: 'postgres', queryCount: 950, errorCount: 2, lastQuery: Date.now() },
          { id: 'shard-02', healthy: true, engine: 'postgres', queryCount: 1100, errorCount: 1, lastQuery: Date.now() },
          { id: 'shard-03', healthy: true, engine: 'postgres', queryCount: 980, errorCount: 0, lastQuery: Date.now() },
        ],
        totalQueries: 4030,
        totalErrors: 3,
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: mockStatus,
        }),
      });

      const status = await client.status();

      expect(status.keyspace).toBe('main');
      expect(status.shardCount).toBe(4);
      expect(status.engine).toBe('postgres');
      expect(status.shards).toHaveLength(4);
      expect(status.totalQueries).toBe(4030);
      expect(status.totalErrors).toBe(3);
    });

    it('should return status with some shards unhealthy', async () => {
      const mockStatus: ClusterStatus = {
        keyspace: 'main',
        shardCount: 3,
        engine: 'sqlite',
        shards: [
          { id: 'shard-00', healthy: true, engine: 'sqlite', queryCount: 500, errorCount: 0, lastQuery: Date.now() },
          { id: 'shard-01', healthy: false, engine: 'sqlite', queryCount: 450, errorCount: 50, lastQuery: Date.now() - 60000 },
          { id: 'shard-02', healthy: true, engine: 'sqlite', queryCount: 520, errorCount: 1, lastQuery: Date.now() },
        ],
        totalQueries: 1470,
        totalErrors: 51,
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: mockStatus,
        }),
      });

      const status = await client.status();

      expect(status.shards[1].healthy).toBe(false);
      const unhealthyShards = status.shards.filter((s) => !s.healthy);
      expect(unhealthyShards).toHaveLength(1);
      expect(unhealthyShards[0].id).toBe('shard-01');
    });

    it('should include latency percentiles when available', async () => {
      const mockShard: ShardHealth = {
        id: 'shard-00',
        healthy: true,
        engine: 'postgres',
        queryCount: 10000,
        errorCount: 5,
        lastQuery: Date.now(),
        latency: {
          p50: 2.5,
          p95: 15.0,
          p99: 45.0,
        },
      };

      const mockStatus: ClusterStatus = {
        keyspace: 'main',
        shardCount: 1,
        engine: 'postgres',
        shards: [mockShard],
        totalQueries: 10000,
        totalErrors: 5,
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: mockStatus,
        }),
      });

      const status = await client.status();

      expect(status.shards[0].latency).toBeDefined();
      expect(status.shards[0].latency!.p50).toBe(2.5);
      expect(status.shards[0].latency!.p95).toBe(15.0);
      expect(status.shards[0].latency!.p99).toBe(45.0);
    });

    it('should handle missing latency data gracefully', async () => {
      const mockStatus: ClusterStatus = {
        keyspace: 'main',
        shardCount: 1,
        engine: 'sqlite',
        shards: [
          { id: 'shard-00', healthy: true, engine: 'sqlite', queryCount: 100, errorCount: 0, lastQuery: Date.now() },
        ],
        totalQueries: 100,
        totalErrors: 0,
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: mockStatus,
        }),
      });

      const status = await client.status();

      expect(status.shards[0].latency).toBeUndefined();
    });

    it('should send STATUS request with correct format', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: {
            keyspace: 'main',
            shardCount: 1,
            engine: 'postgres',
            shards: [],
            totalQueries: 0,
            totalErrors: 0,
          },
        }),
      });

      await client.status();

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.type).toBe(MessageType.STATUS);
      expect(requestBody.id).toBeDefined();
      expect(requestBody.timestamp).toBeDefined();
      expect(requestBody.keyspace).toBe('main');
    });

    it('should throw VitessError on status error response', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'STATUS_UNAVAILABLE',
          message: 'Unable to retrieve cluster status',
        }),
      });

      await expect(client.status()).rejects.toThrow(VitessError);

      try {
        await client.status();
      } catch (e) {
        expect((e as VitessError).code).toBe('STATUS_UNAVAILABLE');
      }
    });

    it('should throw on HTTP error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 500,
        statusText: 'Internal Server Error',
      });

      await expect(client.status()).rejects.toThrow('HTTP 500');
    });
  });

  describe('status() with different storage engines', () => {
    it('should return postgres engine type', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: {
            keyspace: 'main',
            shardCount: 2,
            engine: 'postgres',
            shards: [
              { id: 'shard-00', healthy: true, engine: 'postgres', queryCount: 0, errorCount: 0, lastQuery: 0 },
              { id: 'shard-01', healthy: true, engine: 'postgres', queryCount: 0, errorCount: 0, lastQuery: 0 },
            ],
            totalQueries: 0,
            totalErrors: 0,
          } as ClusterStatus,
        }),
      });

      const status = await client.status();

      expect(status.engine).toBe('postgres');
      expect(status.shards[0].engine).toBe('postgres');
    });

    it('should return sqlite engine type', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: {
            keyspace: 'main',
            shardCount: 2,
            engine: 'sqlite',
            shards: [
              { id: 'shard-00', healthy: true, engine: 'sqlite', queryCount: 0, errorCount: 0, lastQuery: 0 },
              { id: 'shard-01', healthy: true, engine: 'sqlite', queryCount: 0, errorCount: 0, lastQuery: 0 },
            ],
            totalQueries: 0,
            totalErrors: 0,
          } as ClusterStatus,
        }),
      });

      const status = await client.status();

      expect(status.engine).toBe('sqlite');
      expect(status.shards[0].engine).toBe('sqlite');
    });
  });

  describe('status() shard health details', () => {
    it('should include query count per shard', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: {
            keyspace: 'main',
            shardCount: 2,
            engine: 'postgres',
            shards: [
              { id: 'shard-00', healthy: true, engine: 'postgres', queryCount: 5000, errorCount: 10, lastQuery: Date.now() },
              { id: 'shard-01', healthy: true, engine: 'postgres', queryCount: 3500, errorCount: 5, lastQuery: Date.now() },
            ],
            totalQueries: 8500,
            totalErrors: 15,
          } as ClusterStatus,
        }),
      });

      const status = await client.status();

      expect(status.shards[0].queryCount).toBe(5000);
      expect(status.shards[1].queryCount).toBe(3500);
    });

    it('should include error count per shard', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: {
            keyspace: 'main',
            shardCount: 2,
            engine: 'postgres',
            shards: [
              { id: 'shard-00', healthy: true, engine: 'postgres', queryCount: 1000, errorCount: 0, lastQuery: Date.now() },
              { id: 'shard-01', healthy: false, engine: 'postgres', queryCount: 900, errorCount: 100, lastQuery: Date.now() },
            ],
            totalQueries: 1900,
            totalErrors: 100,
          } as ClusterStatus,
        }),
      });

      const status = await client.status();

      expect(status.shards[0].errorCount).toBe(0);
      expect(status.shards[1].errorCount).toBe(100);
    });

    it('should include lastQuery timestamp per shard', async () => {
      const now = Date.now();
      const oneHourAgo = now - 3600000;

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          status: {
            keyspace: 'main',
            shardCount: 2,
            engine: 'postgres',
            shards: [
              { id: 'shard-00', healthy: true, engine: 'postgres', queryCount: 1000, errorCount: 0, lastQuery: now },
              { id: 'shard-01', healthy: true, engine: 'postgres', queryCount: 500, errorCount: 0, lastQuery: oneHourAgo },
            ],
            totalQueries: 1500,
            totalErrors: 0,
          } as ClusterStatus,
        }),
      });

      const status = await client.status();

      expect(status.shards[0].lastQuery).toBe(now);
      expect(status.shards[1].lastQuery).toBe(oneHourAgo);
    });
  });
});

describe('VitessClient VSchema API', () => {
  let client: VitessClient;
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    mockFetch = vi.fn();
    vi.stubGlobal('fetch', mockFetch);

    client = createClient({
      endpoint: 'https://api.vitess.do/v1',
      keyspace: 'commerce',
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

  describe('vschema() method', () => {
    it('should return VSchema configuration for keyspace', async () => {
      const mockVSchema: VSchema = {
        keyspace: 'commerce',
        sharded: true,
        tables: {
          users: {
            vindex: {
              type: 'hash',
              columns: ['tenant_id'],
            },
          },
          orders: {
            vindex: {
              type: 'hash',
              columns: ['tenant_id'],
            },
          },
        },
        vindexes: {
          tenant_hash: {
            type: 'hash',
            columns: ['tenant_id'],
          },
        },
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: mockVSchema,
        }),
      });

      const vschema = await client.vschema();

      expect(vschema.keyspace).toBe('commerce');
      expect(vschema.sharded).toBe(true);
      expect(vschema.tables).toHaveProperty('users');
      expect(vschema.tables).toHaveProperty('orders');
    });

    it('should return unsharded VSchema', async () => {
      const mockVSchema: VSchema = {
        keyspace: 'unsharded_ks',
        sharded: false,
        tables: {
          config: {
            vindex: {
              type: 'null',
              columns: [],
            },
          },
        },
        vindexes: {},
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: mockVSchema,
        }),
      });

      const vschema = await client.vschema();

      expect(vschema.sharded).toBe(false);
    });

    it('should include vindex definitions', async () => {
      const mockVSchema: VSchema = {
        keyspace: 'commerce',
        sharded: true,
        tables: {},
        vindexes: {
          tenant_hash: {
            type: 'hash',
            columns: ['tenant_id'],
          },
          user_lookup: {
            type: 'lookup',
            columns: ['user_id'],
            lookupTable: 'user_lookup_tbl',
          },
        },
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: mockVSchema,
        }),
      });

      const vschema = await client.vschema();

      expect(vschema.vindexes).toHaveProperty('tenant_hash');
      expect(vschema.vindexes.tenant_hash.type).toBe('hash');
      expect(vschema.vindexes).toHaveProperty('user_lookup');
      expect(vschema.vindexes.user_lookup.type).toBe('lookup');
      expect(vschema.vindexes.user_lookup.lookupTable).toBe('user_lookup_tbl');
    });

    it('should include table vindex configuration', async () => {
      const mockVSchema: VSchema = {
        keyspace: 'commerce',
        sharded: true,
        tables: {
          users: {
            vindex: {
              type: 'hash',
              columns: ['tenant_id'],
            },
            autoIncrement: {
              column: 'id',
              sequence: 'users_seq',
            },
          },
        },
        vindexes: {},
      };

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: mockVSchema,
        }),
      });

      const vschema = await client.vschema();

      expect(vschema.tables.users.vindex.type).toBe('hash');
      expect(vschema.tables.users.vindex.columns).toContain('tenant_id');
      expect(vschema.tables.users.autoIncrement).toBeDefined();
      expect(vschema.tables.users.autoIncrement!.column).toBe('id');
      expect(vschema.tables.users.autoIncrement!.sequence).toBe('users_seq');
    });

    it('should send VSCHEMA request with correct format', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: {
            keyspace: 'commerce',
            sharded: true,
            tables: {},
            vindexes: {},
          },
        }),
      });

      await client.vschema();

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.type).toBe(MessageType.VSCHEMA);
      expect(requestBody.id).toBeDefined();
      expect(requestBody.timestamp).toBeDefined();
      expect(requestBody.keyspace).toBe('commerce');
    });

    it('should throw VitessError on vschema error response', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'VSCHEMA_NOT_FOUND',
          message: 'VSchema not configured for keyspace',
        }),
      });

      await expect(client.vschema()).rejects.toThrow(VitessError);

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.ERROR,
          code: 'VSCHEMA_NOT_FOUND',
          message: 'VSchema not configured for keyspace',
        }),
      });

      try {
        await client.vschema();
      } catch (e) {
        expect((e as VitessError).code).toBe('VSCHEMA_NOT_FOUND');
      }
    });

    it('should throw on HTTP error', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: false,
        status: 503,
        statusText: 'Service Unavailable',
      });

      await expect(client.vschema()).rejects.toThrow('HTTP 503');
    });
  });

  describe('vschema() vindex types', () => {
    it('should support hash vindex type', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: {
            keyspace: 'test',
            sharded: true,
            tables: {
              t1: { vindex: { type: 'hash', columns: ['id'] } },
            },
            vindexes: {
              hash_vdx: { type: 'hash', columns: ['id'] },
            },
          } as VSchema,
        }),
      });

      const vschema = await client.vschema();
      expect(vschema.vindexes.hash_vdx.type).toBe('hash');
    });

    it('should support consistent_hash vindex type', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: {
            keyspace: 'test',
            sharded: true,
            tables: {},
            vindexes: {
              ch_vdx: { type: 'consistent_hash', columns: ['user_id'] },
            },
          } as VSchema,
        }),
      });

      const vschema = await client.vschema();
      expect(vschema.vindexes.ch_vdx.type).toBe('consistent_hash');
    });

    it('should support range vindex type', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: {
            keyspace: 'test',
            sharded: true,
            tables: {},
            vindexes: {
              range_vdx: { type: 'range', columns: ['created_at'] },
            },
          } as VSchema,
        }),
      });

      const vschema = await client.vschema();
      expect(vschema.vindexes.range_vdx.type).toBe('range');
    });

    it('should support lookup vindex type', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: {
            keyspace: 'test',
            sharded: true,
            tables: {},
            vindexes: {
              lookup_vdx: {
                type: 'lookup',
                columns: ['email'],
                lookupTable: 'email_to_user',
              },
            },
          } as VSchema,
        }),
      });

      const vschema = await client.vschema();
      expect(vschema.vindexes.lookup_vdx.type).toBe('lookup');
      expect(vschema.vindexes.lookup_vdx.lookupTable).toBe('email_to_user');
    });

    it('should support null vindex type (unsharded)', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: {
            keyspace: 'test',
            sharded: false,
            tables: {
              config: { vindex: { type: 'null', columns: [] } },
            },
            vindexes: {},
          } as VSchema,
        }),
      });

      const vschema = await client.vschema();
      expect(vschema.tables.config.vindex.type).toBe('null');
    });
  });

  describe('vschema() multi-column vindexes', () => {
    it('should support vindexes with multiple columns', async () => {
      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: {
            keyspace: 'test',
            sharded: true,
            tables: {
              events: {
                vindex: {
                  type: 'hash',
                  columns: ['tenant_id', 'region_id'],
                },
              },
            },
            vindexes: {
              composite_vdx: {
                type: 'hash',
                columns: ['tenant_id', 'region_id'],
              },
            },
          } as VSchema,
        }),
      });

      const vschema = await client.vschema();

      expect(vschema.tables.events.vindex.columns).toHaveLength(2);
      expect(vschema.tables.events.vindex.columns).toContain('tenant_id');
      expect(vschema.tables.events.vindex.columns).toContain('region_id');
    });
  });

  describe('vschema() with different keyspaces', () => {
    it('should use keyspace from client config', async () => {
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

      mockFetch.mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          type: MessageType.RESULT,
          vschema: {
            keyspace: 'custom_keyspace',
            sharded: true,
            tables: {},
            vindexes: {},
          } as VSchema,
        }),
      });

      const vschema = await customClient.vschema();

      expect(vschema.keyspace).toBe('custom_keyspace');

      const requestBody = JSON.parse(mockFetch.mock.calls[0][1].body);
      expect(requestBody.keyspace).toBe('custom_keyspace');
    });
  });
});
