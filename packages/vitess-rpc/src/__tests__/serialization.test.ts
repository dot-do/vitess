/**
 * @dotdo/vitess-rpc - Serialization Test Suite (TDD Red)
 *
 * Tests for JSON serialization and deserialization of RPC messages.
 * These tests define expected behavior - implementations to be added.
 */

import { describe, it, expect } from 'vitest';

import { MessageType } from '../protocol.js';

// These will need to be implemented
import {
  serializeRequest,
  serializeResponse,
  deserializeRequest,
  deserializeResponse,
  serializeMessage,
  deserializeMessage,
  safeJsonParse,
  safeJsonStringify,
} from '../serialization.js';

import type {
  QueryRequest,
  ExecuteRequest,
  ErrorResponse,
  QueryResponse,
} from '../protocol.js';

describe('serialization.ts', () => {
  describe('serializeRequest', () => {
    it('should serialize QueryRequest to JSON string', () => {
      const request: QueryRequest = {
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: 1700000000000,
        sql: 'SELECT * FROM users',
        params: [1, 'test'],
      };

      const result = serializeRequest(request);
      expect(typeof result).toBe('string');

      const parsed = JSON.parse(result);
      expect(parsed.type).toBe(MessageType.QUERY);
      expect(parsed.id).toBe('msg-123');
      expect(parsed.sql).toBe('SELECT * FROM users');
      expect(parsed.params).toEqual([1, 'test']);
    });

    it('should serialize ExecuteRequest to JSON string', () => {
      const request: ExecuteRequest = {
        type: MessageType.EXECUTE,
        id: 'msg-456',
        timestamp: 1700000000000,
        sql: 'INSERT INTO users (name) VALUES (?)',
        params: ['John'],
        keyspace: 'main',
      };

      const result = serializeRequest(request);
      const parsed = JSON.parse(result);
      expect(parsed.type).toBe(MessageType.EXECUTE);
      expect(parsed.keyspace).toBe('main');
    });

    it('should handle undefined optional fields', () => {
      const request: QueryRequest = {
        type: MessageType.QUERY,
        id: 'msg-789',
        timestamp: 1700000000000,
        sql: 'SELECT 1',
      };

      const result = serializeRequest(request);
      const parsed = JSON.parse(result);
      expect(parsed.params).toBeUndefined();
      expect(parsed.keyspace).toBeUndefined();
    });

    it('should handle special characters in SQL', () => {
      const request: QueryRequest = {
        type: MessageType.QUERY,
        id: 'msg-special',
        timestamp: 1700000000000,
        sql: 'SELECT * FROM users WHERE name = \'O\'Brien\' AND data = "test"',
      };

      const result = serializeRequest(request);
      const parsed = JSON.parse(result);
      expect(parsed.sql).toBe('SELECT * FROM users WHERE name = \'O\'Brien\' AND data = "test"');
    });

    it('should handle binary data in params as base64', () => {
      const binaryData = new Uint8Array([0x00, 0x01, 0x02, 0xff]);
      const request: QueryRequest = {
        type: MessageType.QUERY,
        id: 'msg-binary',
        timestamp: 1700000000000,
        sql: 'INSERT INTO blobs (data) VALUES (?)',
        params: [binaryData],
      };

      const result = serializeRequest(request);
      expect(typeof result).toBe('string');
      // The implementation should handle binary data encoding
    });
  });

  describe('serializeResponse', () => {
    it('should serialize QueryResponse to JSON string', () => {
      const response: QueryResponse = {
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: 1700000000000,
        result: {
          rows: [{ id: 1, name: 'John' }],
          rowCount: 1,
          fields: [
            { name: 'id', type: 'integer' },
            { name: 'name', type: 'text' },
          ],
        },
      };

      const result = serializeResponse(response);
      const parsed = JSON.parse(result);
      expect(parsed.type).toBe(MessageType.RESULT);
      expect(parsed.result.rows).toHaveLength(1);
      expect(parsed.result.rowCount).toBe(1);
    });

    it('should serialize ErrorResponse to JSON string', () => {
      const response: ErrorResponse = {
        type: MessageType.ERROR,
        id: 'msg-456',
        timestamp: 1700000000000,
        code: 'SQL_ERROR',
        message: 'Syntax error near "FROM"',
        sqlState: '42000',
      };

      const result = serializeResponse(response);
      const parsed = JSON.parse(result);
      expect(parsed.type).toBe(MessageType.ERROR);
      expect(parsed.code).toBe('SQL_ERROR');
      expect(parsed.message).toBe('Syntax error near "FROM"');
    });
  });

  describe('deserializeRequest', () => {
    it('should deserialize valid QueryRequest JSON', () => {
      const json = JSON.stringify({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: 1700000000000,
        sql: 'SELECT * FROM users',
        params: [1],
      });

      const result = deserializeRequest(json);
      expect(result.type).toBe(MessageType.QUERY);
      expect(result.id).toBe('msg-123');
      expect((result as QueryRequest).sql).toBe('SELECT * FROM users');
    });

    it('should throw error for invalid JSON', () => {
      expect(() => deserializeRequest('invalid json')).toThrow();
    });

    it('should throw error for invalid request structure', () => {
      const json = JSON.stringify({ invalid: 'structure' });
      expect(() => deserializeRequest(json)).toThrow();
    });

    it('should throw error for missing required fields', () => {
      const json = JSON.stringify({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: 1700000000000,
        // missing sql
      });
      expect(() => deserializeRequest(json)).toThrow();
    });
  });

  describe('deserializeResponse', () => {
    it('should deserialize valid QueryResponse JSON', () => {
      const json = JSON.stringify({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: 1700000000000,
        result: {
          rows: [{ id: 1 }],
          rowCount: 1,
        },
      });

      const result = deserializeResponse(json);
      expect(result.type).toBe(MessageType.RESULT);
      expect((result as QueryResponse).result.rowCount).toBe(1);
    });

    it('should deserialize valid ErrorResponse JSON', () => {
      const json = JSON.stringify({
        type: MessageType.ERROR,
        id: 'msg-456',
        timestamp: 1700000000000,
        code: 'ERROR',
        message: 'Error message',
      });

      const result = deserializeResponse(json);
      expect(result.type).toBe(MessageType.ERROR);
      expect((result as ErrorResponse).code).toBe('ERROR');
    });

    it('should throw error for invalid JSON', () => {
      expect(() => deserializeResponse('not json')).toThrow();
    });

    it('should throw error for invalid response structure', () => {
      const json = JSON.stringify({ type: 'invalid' });
      expect(() => deserializeResponse(json)).toThrow();
    });
  });

  describe('serializeMessage', () => {
    it('should serialize any valid RPC message', () => {
      const request: QueryRequest = {
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: 1700000000000,
        sql: 'SELECT 1',
      };

      const result = serializeMessage(request);
      expect(typeof result).toBe('string');
      expect(JSON.parse(result).type).toBe(MessageType.QUERY);
    });

    it('should serialize responses', () => {
      const response: ErrorResponse = {
        type: MessageType.ERROR,
        id: 'msg-123',
        timestamp: 1700000000000,
        code: 'ERR',
        message: 'Error',
      };

      const result = serializeMessage(response);
      expect(JSON.parse(result).type).toBe(MessageType.ERROR);
    });
  });

  describe('deserializeMessage', () => {
    it('should deserialize request messages', () => {
      const json = JSON.stringify({
        type: MessageType.QUERY,
        id: 'msg-123',
        timestamp: 1700000000000,
        sql: 'SELECT 1',
      });

      const result = deserializeMessage(json);
      expect(result.type).toBe(MessageType.QUERY);
    });

    it('should deserialize response messages', () => {
      const json = JSON.stringify({
        type: MessageType.RESULT,
        id: 'msg-123',
        timestamp: 1700000000000,
        result: { rows: [], rowCount: 0 },
      });

      const result = deserializeMessage(json);
      expect(result.type).toBe(MessageType.RESULT);
    });

    it('should throw for invalid messages', () => {
      expect(() => deserializeMessage('invalid')).toThrow();
    });
  });

  describe('safeJsonParse', () => {
    it('should return parsed object for valid JSON', () => {
      const result = safeJsonParse('{"key": "value"}');
      expect(result).toEqual({ key: 'value' });
    });

    it('should return null for invalid JSON', () => {
      const result = safeJsonParse('not json');
      expect(result).toBeNull();
    });

    it('should return null for empty string', () => {
      const result = safeJsonParse('');
      expect(result).toBeNull();
    });

    it('should handle arrays', () => {
      const result = safeJsonParse('[1, 2, 3]');
      expect(result).toEqual([1, 2, 3]);
    });

    it('should handle nested objects', () => {
      const result = safeJsonParse('{"nested": {"key": "value"}}');
      expect(result).toEqual({ nested: { key: 'value' } });
    });
  });

  describe('safeJsonStringify', () => {
    it('should return JSON string for valid object', () => {
      const result = safeJsonStringify({ key: 'value' });
      expect(result).toBe('{"key":"value"}');
    });

    it('should return null for circular references', () => {
      const obj: Record<string, unknown> = {};
      obj.self = obj;
      const result = safeJsonStringify(obj);
      expect(result).toBeNull();
    });

    it('should handle undefined values', () => {
      const result = safeJsonStringify({ key: undefined });
      expect(result).toBe('{}');
    });

    it('should handle BigInt by converting to string', () => {
      // BigInt serialization should be handled
      const result = safeJsonStringify({ value: BigInt(12345678901234567890n) });
      expect(result).not.toBeNull();
    });

    it('should handle Date objects', () => {
      const date = new Date('2024-01-01T00:00:00.000Z');
      const result = safeJsonStringify({ date });
      expect(result).toContain('2024-01-01');
    });
  });

  describe('roundtrip serialization', () => {
    it('should preserve QueryRequest through roundtrip', () => {
      const original: QueryRequest = {
        type: MessageType.QUERY,
        id: 'msg-roundtrip',
        timestamp: 1700000000000,
        sql: 'SELECT * FROM users WHERE id = ?',
        params: [42, 'test', true, null],
        keyspace: 'main',
        txId: 'tx-123',
      };

      const serialized = serializeRequest(original);
      const deserialized = deserializeRequest(serialized);

      expect(deserialized).toEqual(original);
    });

    it('should preserve QueryResponse through roundtrip', () => {
      const original: QueryResponse = {
        type: MessageType.RESULT,
        id: 'msg-roundtrip',
        timestamp: 1700000000000,
        result: {
          rows: [
            { id: 1, name: 'John', active: true },
            { id: 2, name: 'Jane', active: false },
          ],
          rowCount: 2,
          fields: [
            { name: 'id', type: 'integer' },
            { name: 'name', type: 'text' },
            { name: 'active', type: 'boolean' },
          ],
          duration: 12.5,
        },
      };

      const serialized = serializeResponse(original);
      const deserialized = deserializeResponse(serialized);

      expect(deserialized).toEqual(original);
    });

    it('should preserve ErrorResponse through roundtrip', () => {
      const original: ErrorResponse = {
        type: MessageType.ERROR,
        id: 'msg-roundtrip',
        timestamp: 1700000000000,
        code: 'SHARD_UNAVAILABLE',
        message: 'Shard shard-0 is currently unavailable',
        shard: 'shard-0',
        sqlState: '08001',
      };

      const serialized = serializeResponse(original);
      const deserialized = deserializeResponse(serialized);

      expect(deserialized).toEqual(original);
    });
  });

  describe('edge cases', () => {
    it('should handle empty rows array', () => {
      const response: QueryResponse = {
        type: MessageType.RESULT,
        id: 'msg-empty',
        timestamp: 1700000000000,
        result: {
          rows: [],
          rowCount: 0,
        },
      };

      const serialized = serializeResponse(response);
      const deserialized = deserializeResponse(serialized);
      expect((deserialized as QueryResponse).result.rows).toEqual([]);
    });

    it('should handle null values in rows', () => {
      const response: QueryResponse = {
        type: MessageType.RESULT,
        id: 'msg-null',
        timestamp: 1700000000000,
        result: {
          rows: [{ id: 1, name: null }],
          rowCount: 1,
        },
      };

      const serialized = serializeResponse(response);
      const deserialized = deserializeResponse(serialized);
      expect((deserialized as QueryResponse).result.rows[0].name).toBeNull();
    });

    it('should handle unicode characters', () => {
      const request: QueryRequest = {
        type: MessageType.QUERY,
        id: 'msg-unicode',
        timestamp: 1700000000000,
        sql: 'SELECT * FROM users WHERE name = ?',
        params: ['Hello'],
      };

      const serialized = serializeRequest(request);
      const deserialized = deserializeRequest(serialized);
      expect((deserialized as QueryRequest).params![0]).toBe('Hello');
    });

    it('should handle very large numbers', () => {
      const response: QueryResponse = {
        type: MessageType.RESULT,
        id: 'msg-large',
        timestamp: 1700000000000,
        result: {
          rows: [{ count: Number.MAX_SAFE_INTEGER }],
          rowCount: 1,
        },
      };

      const serialized = serializeResponse(response);
      const deserialized = deserializeResponse(serialized);
      expect((deserialized as QueryResponse).result.rows[0].count).toBe(Number.MAX_SAFE_INTEGER);
    });
  });
});
