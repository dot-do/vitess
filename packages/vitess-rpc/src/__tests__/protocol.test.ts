/**
 * @dotdo/vitess-rpc - Protocol Test Suite (TDD Red)
 *
 * Tests for RPC message types and MessageType enum.
 * These tests define expected behavior - implementations to be added.
 */

import { describe, it, expect } from 'vitest';

import {
  MessageType,
} from '../protocol.js';

// These will need to be implemented
import {
  isRpcMessage,
  isQueryRequest,
  isExecuteRequest,
  isBatchRequest,
  isBeginRequest,
  isCommitRequest,
  isRollbackRequest,
  isStatusRequest,
  isHealthRequest,
  isSchemaRequest,
  isVSchemaRequest,
  isShardQueryRequest,
  isShardExecuteRequest,
  isShardBatchRequest,
  isQueryResponse,
  isExecuteResponse,
  isBatchResponse,
  isBeginResponse,
  isStatusResponse,
  isHealthResponse,
  isSchemaResponse,
  isVSchemaResponse,
  isErrorResponse,
  isAckResponse,
  isRequest,
  isResponse,
} from '../protocol.js';

describe('protocol.ts', () => {
  describe('MessageType enum', () => {
    it('should have correct values for query operations', () => {
      expect(MessageType.QUERY).toBe(0x01);
      expect(MessageType.EXECUTE).toBe(0x02);
      expect(MessageType.BATCH).toBe(0x03);
    });

    it('should have correct values for transaction operations', () => {
      expect(MessageType.BEGIN).toBe(0x10);
      expect(MessageType.COMMIT).toBe(0x11);
      expect(MessageType.ROLLBACK).toBe(0x12);
    });

    it('should have correct values for admin operations', () => {
      expect(MessageType.STATUS).toBe(0x20);
      expect(MessageType.HEALTH).toBe(0x21);
      expect(MessageType.SCHEMA).toBe(0x22);
      expect(MessageType.VSCHEMA).toBe(0x23);
    });

    it('should have correct values for shard operations', () => {
      expect(MessageType.SHARD_QUERY).toBe(0x30);
      expect(MessageType.SHARD_EXECUTE).toBe(0x31);
      expect(MessageType.SHARD_BATCH).toBe(0x32);
    });

    it('should have correct values for response types', () => {
      expect(MessageType.RESULT).toBe(0x80);
      expect(MessageType.ERROR).toBe(0x81);
      expect(MessageType.ACK).toBe(0x82);
    });
  });

  describe('RpcMessage type guard', () => {
    it('should return true for valid RpcMessage base objects', () => {
      expect(isRpcMessage({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(true);
    });

    it('should return false for invalid RpcMessage objects', () => {
      expect(isRpcMessage({})).toBe(false);
      expect(isRpcMessage({ type: MessageType.QUERY })).toBe(false);
      expect(isRpcMessage({ type: MessageType.QUERY, id: 'msg-123' })).toBe(false);
      expect(isRpcMessage({
        type: 'invalid',
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(false);
      expect(isRpcMessage({
        type: MessageType.QUERY,
        id: 123, // should be string
        timestamp: Date.now(),
      })).toBe(false);
      expect(isRpcMessage(null)).toBe(false);
    });
  });

  describe('QueryRequest type guard', () => {
    it('should return true for valid QueryRequest objects', () => {
      expect(isQueryRequest({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
        sql: 'SELECT * FROM users',
      })).toBe(true);
      expect(isQueryRequest({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
        sql: 'SELECT * FROM users WHERE id = ?',
        params: [1],
        keyspace: 'main',
        txId: 'tx-456',
      })).toBe(true);
    });

    it('should return false for invalid QueryRequest objects', () => {
      expect(isQueryRequest({
        type: MessageType.EXECUTE, // wrong type
        id: 'msg-123',
        timestamp: Date.now(),
        sql: 'SELECT * FROM users',
      })).toBe(false);
      expect(isQueryRequest({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing sql
      })).toBe(false);
      expect(isQueryRequest({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
        sql: '', // empty sql
      })).toBe(false);
      expect(isQueryRequest(null)).toBe(false);
    });
  });

  describe('ExecuteRequest type guard', () => {
    it('should return true for valid ExecuteRequest objects', () => {
      expect(isExecuteRequest({
        type: MessageType.EXECUTE,
        id: 'msg-123',
        timestamp: Date.now(),
        sql: 'INSERT INTO users (name) VALUES (?)',
        params: ['John'],
      })).toBe(true);
    });

    it('should return false for invalid ExecuteRequest objects', () => {
      expect(isExecuteRequest({
        type: MessageType.QUERY, // wrong type
        id: 'msg-123',
        timestamp: Date.now(),
        sql: 'INSERT INTO users (name) VALUES (?)',
      })).toBe(false);
      expect(isExecuteRequest(null)).toBe(false);
    });
  });

  describe('BatchRequest type guard', () => {
    it('should return true for valid BatchRequest objects', () => {
      expect(isBatchRequest({
        type: MessageType.BATCH,
        id: 'msg-123',
        timestamp: Date.now(),
        statements: [
          { sql: 'INSERT INTO users (name) VALUES (?)', params: ['John'] },
          { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Jane'] },
        ],
      })).toBe(true);
    });

    it('should return false for invalid BatchRequest objects', () => {
      expect(isBatchRequest({
        type: MessageType.BATCH,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing statements
      })).toBe(false);
      expect(isBatchRequest({
        type: MessageType.BATCH,
        id: 'msg-123',
        timestamp: Date.now(),
        statements: 'not-array',
      })).toBe(false);
      expect(isBatchRequest(null)).toBe(false);
    });
  });

  describe('BeginRequest type guard', () => {
    it('should return true for valid BeginRequest objects', () => {
      expect(isBeginRequest({
        type: MessageType.BEGIN,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(true);
      expect(isBeginRequest({
        type: MessageType.BEGIN,
        id: 'msg-123',
        timestamp: Date.now(),
        keyspace: 'main',
        options: { isolation: 'serializable' },
      })).toBe(true);
    });

    it('should return false for invalid BeginRequest objects', () => {
      expect(isBeginRequest({
        type: MessageType.COMMIT, // wrong type
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(false);
      expect(isBeginRequest(null)).toBe(false);
    });
  });

  describe('CommitRequest type guard', () => {
    it('should return true for valid CommitRequest objects', () => {
      expect(isCommitRequest({
        type: MessageType.COMMIT,
        id: 'msg-123',
        timestamp: Date.now(),
        txId: 'tx-456',
      })).toBe(true);
    });

    it('should return false for invalid CommitRequest objects', () => {
      expect(isCommitRequest({
        type: MessageType.COMMIT,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing txId
      })).toBe(false);
      expect(isCommitRequest(null)).toBe(false);
    });
  });

  describe('RollbackRequest type guard', () => {
    it('should return true for valid RollbackRequest objects', () => {
      expect(isRollbackRequest({
        type: MessageType.ROLLBACK,
        id: 'msg-123',
        timestamp: Date.now(),
        txId: 'tx-456',
      })).toBe(true);
    });

    it('should return false for invalid RollbackRequest objects', () => {
      expect(isRollbackRequest({
        type: MessageType.ROLLBACK,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing txId
      })).toBe(false);
      expect(isRollbackRequest(null)).toBe(false);
    });
  });

  describe('StatusRequest type guard', () => {
    it('should return true for valid StatusRequest objects', () => {
      expect(isStatusRequest({
        type: MessageType.STATUS,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(true);
      expect(isStatusRequest({
        type: MessageType.STATUS,
        id: 'msg-123',
        timestamp: Date.now(),
        keyspace: 'main',
      })).toBe(true);
    });

    it('should return false for invalid StatusRequest objects', () => {
      expect(isStatusRequest({
        type: MessageType.HEALTH, // wrong type
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(false);
      expect(isStatusRequest(null)).toBe(false);
    });
  });

  describe('HealthRequest type guard', () => {
    it('should return true for valid HealthRequest objects', () => {
      expect(isHealthRequest({
        type: MessageType.HEALTH,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(true);
      expect(isHealthRequest({
        type: MessageType.HEALTH,
        id: 'msg-123',
        timestamp: Date.now(),
        shard: 'shard-0',
      })).toBe(true);
    });

    it('should return false for invalid HealthRequest objects', () => {
      expect(isHealthRequest({
        type: MessageType.STATUS,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(false);
      expect(isHealthRequest(null)).toBe(false);
    });
  });

  describe('SchemaRequest type guard', () => {
    it('should return true for valid SchemaRequest objects', () => {
      expect(isSchemaRequest({
        type: MessageType.SCHEMA,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(true);
    });

    it('should return false for invalid SchemaRequest objects', () => {
      expect(isSchemaRequest({
        type: MessageType.VSCHEMA,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(false);
      expect(isSchemaRequest(null)).toBe(false);
    });
  });

  describe('VSchemaRequest type guard', () => {
    it('should return true for valid VSchemaRequest objects', () => {
      expect(isVSchemaRequest({
        type: MessageType.VSCHEMA,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(true);
    });

    it('should return false for invalid VSchemaRequest objects', () => {
      expect(isVSchemaRequest({
        type: MessageType.SCHEMA,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(false);
      expect(isVSchemaRequest(null)).toBe(false);
    });
  });

  describe('ShardQueryRequest type guard', () => {
    it('should return true for valid ShardQueryRequest objects', () => {
      expect(isShardQueryRequest({
        type: MessageType.SHARD_QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
        shard: 'shard-0',
        sql: 'SELECT * FROM users',
      })).toBe(true);
    });

    it('should return false for invalid ShardQueryRequest objects', () => {
      expect(isShardQueryRequest({
        type: MessageType.SHARD_QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing shard
        sql: 'SELECT * FROM users',
      })).toBe(false);
      expect(isShardQueryRequest({
        type: MessageType.SHARD_QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
        shard: 'shard-0',
        // missing sql
      })).toBe(false);
      expect(isShardQueryRequest(null)).toBe(false);
    });
  });

  describe('ShardExecuteRequest type guard', () => {
    it('should return true for valid ShardExecuteRequest objects', () => {
      expect(isShardExecuteRequest({
        type: MessageType.SHARD_EXECUTE,
        id: 'msg-123',
        timestamp: Date.now(),
        shard: 'shard-0',
        sql: 'INSERT INTO users (name) VALUES (?)',
        params: ['John'],
      })).toBe(true);
    });

    it('should return false for invalid ShardExecuteRequest objects', () => {
      expect(isShardExecuteRequest({
        type: MessageType.SHARD_EXECUTE,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing shard and sql
      })).toBe(false);
      expect(isShardExecuteRequest(null)).toBe(false);
    });
  });

  describe('ShardBatchRequest type guard', () => {
    it('should return true for valid ShardBatchRequest objects', () => {
      expect(isShardBatchRequest({
        type: MessageType.SHARD_BATCH,
        id: 'msg-123',
        timestamp: Date.now(),
        shard: 'shard-0',
        statements: [{ sql: 'INSERT INTO users (name) VALUES (?)', params: ['John'] }],
      })).toBe(true);
    });

    it('should return false for invalid ShardBatchRequest objects', () => {
      expect(isShardBatchRequest({
        type: MessageType.SHARD_BATCH,
        id: 'msg-123',
        timestamp: Date.now(),
        shard: 'shard-0',
        // missing statements
      })).toBe(false);
      expect(isShardBatchRequest(null)).toBe(false);
    });
  });

  describe('QueryResponse type guard', () => {
    it('should return true for valid QueryResponse objects', () => {
      expect(isQueryResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        result: { rows: [{ id: 1 }], rowCount: 1 },
      })).toBe(true);
    });

    it('should return false for invalid QueryResponse objects', () => {
      expect(isQueryResponse({
        type: MessageType.ERROR, // wrong type
        id: 'msg-123',
        timestamp: Date.now(),
        result: { rows: [], rowCount: 0 },
      })).toBe(false);
      expect(isQueryResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing result
      })).toBe(false);
      expect(isQueryResponse(null)).toBe(false);
    });
  });

  describe('ExecuteResponse type guard', () => {
    it('should return true for valid ExecuteResponse objects', () => {
      expect(isExecuteResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        result: { affected: 1 },
      })).toBe(true);
    });

    it('should return false for invalid ExecuteResponse objects', () => {
      expect(isExecuteResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        result: { rows: [] }, // wrong result structure
      })).toBe(false);
      expect(isExecuteResponse(null)).toBe(false);
    });
  });

  describe('BatchResponse type guard', () => {
    it('should return true for valid BatchResponse objects', () => {
      expect(isBatchResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        result: { results: [], success: true },
      })).toBe(true);
    });

    it('should return false for invalid BatchResponse objects', () => {
      expect(isBatchResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        result: { rows: [] }, // wrong result structure
      })).toBe(false);
      expect(isBatchResponse(null)).toBe(false);
    });
  });

  describe('BeginResponse type guard', () => {
    it('should return true for valid BeginResponse objects', () => {
      expect(isBeginResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        txId: 'tx-456',
        shards: ['shard-0', 'shard-1'],
      })).toBe(true);
    });

    it('should return false for invalid BeginResponse objects', () => {
      expect(isBeginResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing txId
        shards: [],
      })).toBe(false);
      expect(isBeginResponse(null)).toBe(false);
    });
  });

  describe('StatusResponse type guard', () => {
    it('should return true for valid StatusResponse objects', () => {
      expect(isStatusResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        status: {
          keyspace: 'main',
          shardCount: 4,
          engine: 'postgres',
          shards: [],
          totalQueries: 0,
          totalErrors: 0,
        },
      })).toBe(true);
    });

    it('should return false for invalid StatusResponse objects', () => {
      expect(isStatusResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing status
      })).toBe(false);
      expect(isStatusResponse(null)).toBe(false);
    });
  });

  describe('HealthResponse type guard', () => {
    it('should return true for valid HealthResponse objects', () => {
      expect(isHealthResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        health: {
          id: 'shard-0',
          healthy: true,
          engine: 'postgres',
          queryCount: 0,
          errorCount: 0,
          lastQuery: Date.now(),
        },
      })).toBe(true);
      expect(isHealthResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        health: [
          { id: 'shard-0', healthy: true, engine: 'postgres', queryCount: 0, errorCount: 0, lastQuery: Date.now() },
        ],
      })).toBe(true);
    });

    it('should return false for invalid HealthResponse objects', () => {
      expect(isHealthResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing health
      })).toBe(false);
      expect(isHealthResponse(null)).toBe(false);
    });
  });

  describe('SchemaResponse type guard', () => {
    it('should return true for valid SchemaResponse objects', () => {
      expect(isSchemaResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        tables: [
          { name: 'users', type: 'table', columns: [{ name: 'id', type: 'integer', nullable: false }] },
        ],
      })).toBe(true);
    });

    it('should return false for invalid SchemaResponse objects', () => {
      expect(isSchemaResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing tables
      })).toBe(false);
      expect(isSchemaResponse(null)).toBe(false);
    });
  });

  describe('VSchemaResponse type guard', () => {
    it('should return true for valid VSchemaResponse objects', () => {
      expect(isVSchemaResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        vschema: {
          keyspace: 'main',
          sharded: false,
          tables: {},
          vindexes: {},
        },
      })).toBe(true);
    });

    it('should return false for invalid VSchemaResponse objects', () => {
      expect(isVSchemaResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing vschema
      })).toBe(false);
      expect(isVSchemaResponse(null)).toBe(false);
    });
  });

  describe('ErrorResponse type guard', () => {
    it('should return true for valid ErrorResponse objects', () => {
      expect(isErrorResponse({
        type: MessageType.ERROR,
        id: 'msg-123',
        timestamp: Date.now(),
        code: 'SQL_ERROR',
        message: 'Syntax error in SQL',
      })).toBe(true);
      expect(isErrorResponse({
        type: MessageType.ERROR,
        id: 'msg-123',
        timestamp: Date.now(),
        code: 'SHARD_ERROR',
        message: 'Shard unavailable',
        shard: 'shard-0',
        sqlState: '42000',
      })).toBe(true);
    });

    it('should return false for invalid ErrorResponse objects', () => {
      expect(isErrorResponse({
        type: MessageType.RESULT, // wrong type
        id: 'msg-123',
        timestamp: Date.now(),
        code: 'ERROR',
        message: 'Error',
      })).toBe(false);
      expect(isErrorResponse({
        type: MessageType.ERROR,
        id: 'msg-123',
        timestamp: Date.now(),
        // missing code and message
      })).toBe(false);
      expect(isErrorResponse(null)).toBe(false);
    });
  });

  describe('AckResponse type guard', () => {
    it('should return true for valid AckResponse objects', () => {
      expect(isAckResponse({
        type: MessageType.ACK,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(true);
    });

    it('should return false for invalid AckResponse objects', () => {
      expect(isAckResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(false);
      expect(isAckResponse(null)).toBe(false);
    });
  });

  describe('Request type guard', () => {
    it('should return true for any valid request type', () => {
      expect(isRequest({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
        sql: 'SELECT 1',
      })).toBe(true);
      expect(isRequest({
        type: MessageType.EXECUTE,
        id: 'msg-123',
        timestamp: Date.now(),
        sql: 'INSERT INTO x VALUES (1)',
      })).toBe(true);
      expect(isRequest({
        type: MessageType.BEGIN,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(true);
    });

    it('should return false for response types', () => {
      expect(isRequest({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        result: { rows: [], rowCount: 0 },
      })).toBe(false);
      expect(isRequest({
        type: MessageType.ERROR,
        id: 'msg-123',
        timestamp: Date.now(),
        code: 'ERROR',
        message: 'Error',
      })).toBe(false);
    });
  });

  describe('Response type guard', () => {
    it('should return true for any valid response type', () => {
      expect(isResponse({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: Date.now(),
        result: { rows: [], rowCount: 0 },
      })).toBe(true);
      expect(isResponse({
        type: MessageType.ERROR,
        id: 'msg-123',
        timestamp: Date.now(),
        code: 'ERROR',
        message: 'Error',
      })).toBe(true);
      expect(isResponse({
        type: MessageType.ACK,
        id: 'msg-123',
        timestamp: Date.now(),
      })).toBe(true);
    });

    it('should return false for request types', () => {
      expect(isResponse({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: Date.now(),
        sql: 'SELECT 1',
      })).toBe(false);
    });
  });
});
