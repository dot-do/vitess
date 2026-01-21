/**
 * @dotdo/vitess-rpc - Factories Test Suite (TDD Red)
 *
 * Tests for factory functions that create RPC messages.
 * These tests define expected behavior - implementations to be added.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import {
  MessageType,
  createMessageId,
  createQueryRequest,
  createExecuteRequest,
  createErrorResponse,
} from '../protocol.js';

// These will need to be implemented
import {
  createBatchRequest,
  createBeginRequest,
  createCommitRequest,
  createRollbackRequest,
  createStatusRequest,
  createHealthRequest,
  createSchemaRequest,
  createVSchemaRequest,
  createShardQueryRequest,
  createShardExecuteRequest,
  createShardBatchRequest,
  createQueryResponse,
  createExecuteResponse,
  createBatchResponse,
  createBeginResponse,
  createStatusResponse,
  createHealthResponse,
  createSchemaResponse,
  createVSchemaResponse,
  createAckResponse,
} from '../protocol.js';

describe('factories.ts', () => {
  describe('createMessageId', () => {
    it('should generate unique IDs', () => {
      const id1 = createMessageId();
      const id2 = createMessageId();
      const id3 = createMessageId();

      expect(id1).not.toBe(id2);
      expect(id2).not.toBe(id3);
      expect(id1).not.toBe(id3);
    });

    it('should generate string IDs', () => {
      const id = createMessageId();
      expect(typeof id).toBe('string');
    });

    it('should generate non-empty IDs', () => {
      const id = createMessageId();
      expect(id.length).toBeGreaterThan(0);
    });

    it('should include timestamp component', () => {
      const before = Date.now();
      const id = createMessageId();
      const after = Date.now();

      // The ID should contain a timestamp that falls within the before/after range
      const timestampPart = id.split('-')[0];
      const timestamp = parseInt(timestampPart, 10);
      expect(timestamp).toBeGreaterThanOrEqual(before);
      expect(timestamp).toBeLessThanOrEqual(after);
    });
  });

  describe('createQueryRequest', () => {
    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2024-01-01T00:00:00.000Z'));
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('should create a valid QueryRequest with required fields', () => {
      const request = createQueryRequest('SELECT * FROM users');

      expect(request.type).toBe(MessageType.QUERY);
      expect(request.sql).toBe('SELECT * FROM users');
      expect(typeof request.id).toBe('string');
      expect(request.timestamp).toBe(Date.now());
    });

    it('should create QueryRequest with params', () => {
      const request = createQueryRequest('SELECT * FROM users WHERE id = ?', [1]);

      expect(request.params).toEqual([1]);
    });

    it('should create QueryRequest with options', () => {
      const request = createQueryRequest('SELECT * FROM users', undefined, {
        keyspace: 'main',
        txId: 'tx-123',
      });

      expect(request.keyspace).toBe('main');
      expect(request.txId).toBe('tx-123');
    });

    it('should create QueryRequest with all fields', () => {
      const request = createQueryRequest('SELECT * FROM users WHERE id = ?', [1], {
        keyspace: 'main',
        txId: 'tx-456',
      });

      expect(request.type).toBe(MessageType.QUERY);
      expect(request.sql).toBe('SELECT * FROM users WHERE id = ?');
      expect(request.params).toEqual([1]);
      expect(request.keyspace).toBe('main');
      expect(request.txId).toBe('tx-456');
    });
  });

  describe('createExecuteRequest', () => {
    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2024-01-01T00:00:00.000Z'));
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('should create a valid ExecuteRequest', () => {
      const request = createExecuteRequest('INSERT INTO users (name) VALUES (?)', ['John']);

      expect(request.type).toBe(MessageType.EXECUTE);
      expect(request.sql).toBe('INSERT INTO users (name) VALUES (?)');
      expect(request.params).toEqual(['John']);
    });

    it('should create ExecuteRequest with options', () => {
      const request = createExecuteRequest('DELETE FROM users WHERE id = ?', [1], {
        keyspace: 'commerce',
        txId: 'tx-789',
      });

      expect(request.keyspace).toBe('commerce');
      expect(request.txId).toBe('tx-789');
    });
  });

  describe('createBatchRequest', () => {
    it('should create a valid BatchRequest', () => {
      const request = createBatchRequest([
        { sql: 'INSERT INTO users (name) VALUES (?)', params: ['John'] },
        { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Jane'] },
      ]);

      expect(request.type).toBe(MessageType.BATCH);
      expect(request.statements).toHaveLength(2);
      expect(request.statements[0].sql).toBe('INSERT INTO users (name) VALUES (?)');
    });

    it('should create BatchRequest with options', () => {
      const request = createBatchRequest(
        [{ sql: 'SELECT 1' }],
        { keyspace: 'main', txId: 'tx-batch' }
      );

      expect(request.keyspace).toBe('main');
      expect(request.txId).toBe('tx-batch');
    });

    it('should create BatchRequest with empty statements', () => {
      const request = createBatchRequest([]);

      expect(request.statements).toEqual([]);
    });
  });

  describe('createBeginRequest', () => {
    it('should create a valid BeginRequest', () => {
      const request = createBeginRequest();

      expect(request.type).toBe(MessageType.BEGIN);
      expect(typeof request.id).toBe('string');
      expect(typeof request.timestamp).toBe('number');
    });

    it('should create BeginRequest with keyspace', () => {
      const request = createBeginRequest({ keyspace: 'main' });

      expect(request.keyspace).toBe('main');
    });

    it('should create BeginRequest with transaction options', () => {
      const request = createBeginRequest({
        keyspace: 'main',
        options: {
          isolation: 'serializable',
          readOnly: true,
          timeout: 5000,
        },
      });

      expect(request.options?.isolation).toBe('serializable');
      expect(request.options?.readOnly).toBe(true);
      expect(request.options?.timeout).toBe(5000);
    });
  });

  describe('createCommitRequest', () => {
    it('should create a valid CommitRequest', () => {
      const request = createCommitRequest('tx-123');

      expect(request.type).toBe(MessageType.COMMIT);
      expect(request.txId).toBe('tx-123');
    });
  });

  describe('createRollbackRequest', () => {
    it('should create a valid RollbackRequest', () => {
      const request = createRollbackRequest('tx-456');

      expect(request.type).toBe(MessageType.ROLLBACK);
      expect(request.txId).toBe('tx-456');
    });
  });

  describe('createStatusRequest', () => {
    it('should create a valid StatusRequest', () => {
      const request = createStatusRequest();

      expect(request.type).toBe(MessageType.STATUS);
    });

    it('should create StatusRequest with keyspace', () => {
      const request = createStatusRequest({ keyspace: 'main' });

      expect(request.keyspace).toBe('main');
    });
  });

  describe('createHealthRequest', () => {
    it('should create a valid HealthRequest', () => {
      const request = createHealthRequest();

      expect(request.type).toBe(MessageType.HEALTH);
    });

    it('should create HealthRequest for specific shard', () => {
      const request = createHealthRequest({ shard: 'shard-0' });

      expect(request.shard).toBe('shard-0');
    });
  });

  describe('createSchemaRequest', () => {
    it('should create a valid SchemaRequest', () => {
      const request = createSchemaRequest();

      expect(request.type).toBe(MessageType.SCHEMA);
    });

    it('should create SchemaRequest with keyspace', () => {
      const request = createSchemaRequest({ keyspace: 'commerce' });

      expect(request.keyspace).toBe('commerce');
    });
  });

  describe('createVSchemaRequest', () => {
    it('should create a valid VSchemaRequest', () => {
      const request = createVSchemaRequest();

      expect(request.type).toBe(MessageType.VSCHEMA);
    });

    it('should create VSchemaRequest with keyspace', () => {
      const request = createVSchemaRequest({ keyspace: 'commerce' });

      expect(request.keyspace).toBe('commerce');
    });
  });

  describe('createShardQueryRequest', () => {
    it('should create a valid ShardQueryRequest', () => {
      const request = createShardQueryRequest('shard-0', 'SELECT * FROM users');

      expect(request.type).toBe(MessageType.SHARD_QUERY);
      expect(request.shard).toBe('shard-0');
      expect(request.sql).toBe('SELECT * FROM users');
    });

    it('should create ShardQueryRequest with params', () => {
      const request = createShardQueryRequest('shard-1', 'SELECT * FROM users WHERE id = ?', [42]);

      expect(request.params).toEqual([42]);
    });
  });

  describe('createShardExecuteRequest', () => {
    it('should create a valid ShardExecuteRequest', () => {
      const request = createShardExecuteRequest('shard-0', 'INSERT INTO users (name) VALUES (?)', ['John']);

      expect(request.type).toBe(MessageType.SHARD_EXECUTE);
      expect(request.shard).toBe('shard-0');
      expect(request.sql).toBe('INSERT INTO users (name) VALUES (?)');
      expect(request.params).toEqual(['John']);
    });
  });

  describe('createShardBatchRequest', () => {
    it('should create a valid ShardBatchRequest', () => {
      const request = createShardBatchRequest('shard-0', [
        { sql: 'INSERT INTO users (name) VALUES (?)', params: ['John'] },
      ]);

      expect(request.type).toBe(MessageType.SHARD_BATCH);
      expect(request.shard).toBe('shard-0');
      expect(request.statements).toHaveLength(1);
    });
  });

  describe('createErrorResponse', () => {
    it('should create a valid ErrorResponse', () => {
      const response = createErrorResponse('req-123', 'SQL_ERROR', 'Syntax error');

      expect(response.type).toBe(MessageType.ERROR);
      expect(response.id).toBe('req-123');
      expect(response.code).toBe('SQL_ERROR');
      expect(response.message).toBe('Syntax error');
    });

    it('should create ErrorResponse with shard info', () => {
      const response = createErrorResponse('req-456', 'SHARD_ERROR', 'Shard unavailable', {
        shard: 'shard-0',
      });

      expect(response.shard).toBe('shard-0');
    });

    it('should create ErrorResponse with sqlState', () => {
      const response = createErrorResponse('req-789', 'SQL_ERROR', 'Error', {
        sqlState: '42000',
      });

      expect(response.sqlState).toBe('42000');
    });
  });

  describe('createQueryResponse', () => {
    it('should create a valid QueryResponse', () => {
      const response = createQueryResponse('req-123', {
        rows: [{ id: 1, name: 'John' }],
        rowCount: 1,
      });

      expect(response.type).toBe(MessageType.RESULT);
      expect(response.id).toBe('req-123');
      expect(response.result.rows).toHaveLength(1);
      expect(response.result.rowCount).toBe(1);
    });

    it('should create QueryResponse with fields', () => {
      const response = createQueryResponse('req-456', {
        rows: [],
        rowCount: 0,
        fields: [{ name: 'id', type: 'integer' }],
      });

      expect(response.result.fields).toHaveLength(1);
    });

    it('should create QueryResponse with duration', () => {
      const response = createQueryResponse('req-789', {
        rows: [],
        rowCount: 0,
        duration: 15.5,
      });

      expect(response.result.duration).toBe(15.5);
    });
  });

  describe('createExecuteResponse', () => {
    it('should create a valid ExecuteResponse', () => {
      const response = createExecuteResponse('req-123', {
        affected: 1,
      });

      expect(response.type).toBe(MessageType.RESULT);
      expect(response.result.affected).toBe(1);
    });

    it('should create ExecuteResponse with lastInsertId', () => {
      const response = createExecuteResponse('req-456', {
        affected: 1,
        lastInsertId: '42',
      });

      expect(response.result.lastInsertId).toBe('42');
    });
  });

  describe('createBatchResponse', () => {
    it('should create a valid BatchResponse', () => {
      const response = createBatchResponse('req-123', {
        results: [{ rows: [], rowCount: 0 }],
        success: true,
      });

      expect(response.type).toBe(MessageType.RESULT);
      expect(response.result.success).toBe(true);
    });

    it('should create BatchResponse with failure info', () => {
      const response = createBatchResponse('req-456', {
        results: [{ rows: [], rowCount: 0 }],
        success: false,
        failedAt: 1,
        error: 'SQL syntax error',
      });

      expect(response.result.success).toBe(false);
      expect(response.result.failedAt).toBe(1);
      expect(response.result.error).toBe('SQL syntax error');
    });
  });

  describe('createBeginResponse', () => {
    it('should create a valid BeginResponse', () => {
      const response = createBeginResponse('req-123', 'tx-new-456', ['shard-0', 'shard-1']);

      expect(response.type).toBe(MessageType.RESULT);
      expect(response.id).toBe('req-123');
      expect(response.txId).toBe('tx-new-456');
      expect(response.shards).toEqual(['shard-0', 'shard-1']);
    });
  });

  describe('createStatusResponse', () => {
    it('should create a valid StatusResponse', () => {
      const response = createStatusResponse('req-123', {
        keyspace: 'main',
        shardCount: 4,
        engine: 'postgres',
        shards: [],
        totalQueries: 100,
        totalErrors: 0,
      });

      expect(response.type).toBe(MessageType.RESULT);
      expect(response.status.keyspace).toBe('main');
      expect(response.status.shardCount).toBe(4);
    });
  });

  describe('createHealthResponse', () => {
    it('should create a valid HealthResponse with single shard', () => {
      const response = createHealthResponse('req-123', {
        id: 'shard-0',
        healthy: true,
        engine: 'postgres',
        queryCount: 50,
        errorCount: 0,
        lastQuery: Date.now(),
      });

      expect(response.type).toBe(MessageType.RESULT);
      expect(response.health).not.toBeInstanceOf(Array);
    });

    it('should create HealthResponse with multiple shards', () => {
      const response = createHealthResponse('req-456', [
        { id: 'shard-0', healthy: true, engine: 'postgres', queryCount: 50, errorCount: 0, lastQuery: Date.now() },
        { id: 'shard-1', healthy: true, engine: 'postgres', queryCount: 40, errorCount: 1, lastQuery: Date.now() },
      ]);

      expect(response.type).toBe(MessageType.RESULT);
      expect(Array.isArray(response.health)).toBe(true);
      expect((response.health as unknown[]).length).toBe(2);
    });
  });

  describe('createSchemaResponse', () => {
    it('should create a valid SchemaResponse', () => {
      const response = createSchemaResponse('req-123', [
        {
          name: 'users',
          type: 'table',
          columns: [
            { name: 'id', type: 'integer', nullable: false },
            { name: 'name', type: 'text', nullable: true },
          ],
        },
      ]);

      expect(response.type).toBe(MessageType.RESULT);
      expect(response.tables).toHaveLength(1);
      expect(response.tables[0].name).toBe('users');
    });
  });

  describe('createVSchemaResponse', () => {
    it('should create a valid VSchemaResponse', () => {
      const response = createVSchemaResponse('req-123', {
        keyspace: 'main',
        sharded: true,
        tables: {
          users: {
            vindex: { type: 'hash', columns: ['id'] },
          },
        },
        vindexes: {
          user_hash: { type: 'hash', columns: ['id'] },
        },
      });

      expect(response.type).toBe(MessageType.RESULT);
      expect(response.vschema.keyspace).toBe('main');
      expect(response.vschema.sharded).toBe(true);
    });
  });

  describe('createAckResponse', () => {
    it('should create a valid AckResponse', () => {
      const response = createAckResponse('req-123');

      expect(response.type).toBe(MessageType.ACK);
      expect(response.id).toBe('req-123');
      expect(typeof response.timestamp).toBe('number');
    });
  });

  describe('factory function consistency', () => {
    it('should always include required RpcMessage fields', () => {
      const factories = [
        () => createQueryRequest('SELECT 1'),
        () => createExecuteRequest('INSERT INTO x VALUES (1)'),
        () => createBatchRequest([]),
        () => createBeginRequest(),
        () => createCommitRequest('tx-1'),
        () => createRollbackRequest('tx-1'),
        () => createStatusRequest(),
        () => createHealthRequest(),
        () => createSchemaRequest(),
        () => createVSchemaRequest(),
        () => createShardQueryRequest('shard-0', 'SELECT 1'),
        () => createShardExecuteRequest('shard-0', 'INSERT INTO x VALUES (1)'),
        () => createShardBatchRequest('shard-0', []),
      ];

      for (const factory of factories) {
        const message = factory();
        expect(message).toHaveProperty('type');
        expect(message).toHaveProperty('id');
        expect(message).toHaveProperty('timestamp');
        expect(typeof message.id).toBe('string');
        expect(typeof message.timestamp).toBe('number');
      }
    });

    it('should generate unique IDs for each request', () => {
      const ids = new Set<string>();
      for (let i = 0; i < 100; i++) {
        const request = createQueryRequest('SELECT 1');
        ids.add(request.id);
      }
      expect(ids.size).toBe(100);
    });
  });
});
