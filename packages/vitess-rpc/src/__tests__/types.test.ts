/**
 * @dotdo/vitess-rpc - Types Test Suite (TDD Red)
 *
 * Tests for type guards and validation functions.
 * These tests define expected behavior - implementations to be added.
 */

import { describe, it, expect } from 'vitest';

// These will need to be implemented
import {
  isRow,
  isField,
  isQueryResult,
  isExecuteResult,
  isBatchResult,
  isShardHealth,
  isClusterStatus,
  validateField,
  validateQueryResult,
  validateExecuteResult,
} from '../types.js';

describe('types.ts', () => {
  describe('Row type guard', () => {
    it('should return true for valid Row objects', () => {
      expect(isRow({})).toBe(true);
      expect(isRow({ id: 1, name: 'test' })).toBe(true);
      expect(isRow({ complex: { nested: 'value' } })).toBe(true);
    });

    it('should return false for non-object values', () => {
      expect(isRow(null)).toBe(false);
      expect(isRow(undefined)).toBe(false);
      expect(isRow('string')).toBe(false);
      expect(isRow(123)).toBe(false);
      expect(isRow([])).toBe(false);
    });
  });

  describe('Field type guard', () => {
    it('should return true for valid Field objects', () => {
      expect(isField({ name: 'id', type: 'integer' })).toBe(true);
      expect(isField({ name: 'name', type: 'text', nativeType: 'varchar' })).toBe(true);
      expect(isField({ name: 'count', type: 'integer', nativeType: 23 })).toBe(true);
    });

    it('should return false for invalid Field objects', () => {
      expect(isField({})).toBe(false);
      expect(isField({ name: 'test' })).toBe(false);
      expect(isField({ type: 'text' })).toBe(false);
      expect(isField({ name: 123, type: 'text' })).toBe(false);
      expect(isField({ name: 'test', type: 456 })).toBe(false);
      expect(isField(null)).toBe(false);
    });
  });

  describe('QueryResult type guard', () => {
    it('should return true for valid QueryResult objects', () => {
      expect(isQueryResult({ rows: [], rowCount: 0 })).toBe(true);
      expect(isQueryResult({
        rows: [{ id: 1 }, { id: 2 }],
        rowCount: 2,
      })).toBe(true);
      expect(isQueryResult({
        rows: [{ id: 1 }],
        rowCount: 1,
        fields: [{ name: 'id', type: 'integer' }],
      })).toBe(true);
      expect(isQueryResult({
        rows: [],
        rowCount: 0,
        duration: 123.45,
      })).toBe(true);
    });

    it('should return false for invalid QueryResult objects', () => {
      expect(isQueryResult({})).toBe(false);
      expect(isQueryResult({ rows: [] })).toBe(false);
      expect(isQueryResult({ rowCount: 0 })).toBe(false);
      expect(isQueryResult({ rows: 'not-array', rowCount: 0 })).toBe(false);
      expect(isQueryResult({ rows: [], rowCount: 'not-number' })).toBe(false);
      expect(isQueryResult(null)).toBe(false);
    });

    it('should validate fields array if present', () => {
      expect(isQueryResult({
        rows: [],
        rowCount: 0,
        fields: [{ name: 'id', type: 'integer' }],
      })).toBe(true);
      expect(isQueryResult({
        rows: [],
        rowCount: 0,
        fields: [{ invalid: 'field' }],
      })).toBe(false);
    });
  });

  describe('ExecuteResult type guard', () => {
    it('should return true for valid ExecuteResult objects', () => {
      expect(isExecuteResult({ affected: 0 })).toBe(true);
      expect(isExecuteResult({ affected: 5 })).toBe(true);
      expect(isExecuteResult({ affected: 1, lastInsertId: '123' })).toBe(true);
      expect(isExecuteResult({ affected: 1, lastInsertId: 456 })).toBe(true);
    });

    it('should return false for invalid ExecuteResult objects', () => {
      expect(isExecuteResult({})).toBe(false);
      expect(isExecuteResult({ affected: 'not-number' })).toBe(false);
      expect(isExecuteResult({ affected: -1 })).toBe(false);
      expect(isExecuteResult(null)).toBe(false);
    });
  });

  describe('BatchResult type guard', () => {
    it('should return true for valid BatchResult objects', () => {
      expect(isBatchResult({
        results: [],
        success: true,
      })).toBe(true);
      expect(isBatchResult({
        results: [{ rows: [], rowCount: 0 }],
        success: true,
      })).toBe(true);
      expect(isBatchResult({
        results: [],
        success: false,
        failedAt: 2,
        error: 'SQL syntax error',
      })).toBe(true);
    });

    it('should return false for invalid BatchResult objects', () => {
      expect(isBatchResult({})).toBe(false);
      expect(isBatchResult({ results: [] })).toBe(false);
      expect(isBatchResult({ success: true })).toBe(false);
      expect(isBatchResult({ results: 'not-array', success: true })).toBe(false);
      expect(isBatchResult(null)).toBe(false);
    });
  });

  describe('ShardHealth type guard', () => {
    it('should return true for valid ShardHealth objects', () => {
      expect(isShardHealth({
        id: 'shard-0',
        healthy: true,
        engine: 'postgres',
        queryCount: 100,
        errorCount: 0,
        lastQuery: Date.now(),
      })).toBe(true);
      expect(isShardHealth({
        id: 'shard-1',
        healthy: false,
        engine: 'sqlite',
        queryCount: 50,
        errorCount: 5,
        lastQuery: Date.now(),
        latency: { p50: 10, p95: 50, p99: 100 },
      })).toBe(true);
    });

    it('should return false for invalid ShardHealth objects', () => {
      expect(isShardHealth({})).toBe(false);
      expect(isShardHealth({ id: 'shard-0' })).toBe(false);
      expect(isShardHealth({
        id: 'shard-0',
        healthy: 'yes', // should be boolean
        engine: 'postgres',
        queryCount: 0,
        errorCount: 0,
        lastQuery: Date.now(),
      })).toBe(false);
      expect(isShardHealth(null)).toBe(false);
    });

    it('should validate latency object if present', () => {
      expect(isShardHealth({
        id: 'shard-0',
        healthy: true,
        engine: 'postgres',
        queryCount: 0,
        errorCount: 0,
        lastQuery: Date.now(),
        latency: { p50: 'not-number', p95: 50, p99: 100 },
      })).toBe(false);
    });
  });

  describe('ClusterStatus type guard', () => {
    it('should return true for valid ClusterStatus objects', () => {
      expect(isClusterStatus({
        keyspace: 'main',
        shardCount: 4,
        engine: 'postgres',
        shards: [],
        totalQueries: 0,
        totalErrors: 0,
      })).toBe(true);
      expect(isClusterStatus({
        keyspace: 'test',
        shardCount: 1,
        engine: 'sqlite',
        shards: [{
          id: 'shard-0',
          healthy: true,
          engine: 'sqlite',
          queryCount: 10,
          errorCount: 0,
          lastQuery: Date.now(),
        }],
        totalQueries: 10,
        totalErrors: 0,
      })).toBe(true);
    });

    it('should return false for invalid ClusterStatus objects', () => {
      expect(isClusterStatus({})).toBe(false);
      expect(isClusterStatus({ keyspace: 'main' })).toBe(false);
      expect(isClusterStatus(null)).toBe(false);
    });
  });

  describe('validateField', () => {
    it('should return null for valid fields', () => {
      expect(validateField({ name: 'id', type: 'integer' })).toBeNull();
    });

    it('should return error message for missing name', () => {
      const error = validateField({ type: 'integer' });
      expect(error).toContain('name');
    });

    it('should return error message for missing type', () => {
      const error = validateField({ name: 'id' });
      expect(error).toContain('type');
    });

    it('should return error message for invalid nativeType', () => {
      const error = validateField({ name: 'id', type: 'integer', nativeType: true });
      expect(error).toContain('nativeType');
    });
  });

  describe('validateQueryResult', () => {
    it('should return null for valid QueryResult', () => {
      expect(validateQueryResult({ rows: [], rowCount: 0 })).toBeNull();
    });

    it('should return error message for missing rows', () => {
      const error = validateQueryResult({ rowCount: 0 });
      expect(error).toContain('rows');
    });

    it('should return error message for invalid rows type', () => {
      const error = validateQueryResult({ rows: 'invalid', rowCount: 0 });
      expect(error).toContain('rows');
    });

    it('should return error message for missing rowCount', () => {
      const error = validateQueryResult({ rows: [] });
      expect(error).toContain('rowCount');
    });

    it('should return error message for negative rowCount', () => {
      const error = validateQueryResult({ rows: [], rowCount: -1 });
      expect(error).toContain('rowCount');
    });

    it('should return error message for negative duration', () => {
      const error = validateQueryResult({ rows: [], rowCount: 0, duration: -1 });
      expect(error).toContain('duration');
    });
  });

  describe('validateExecuteResult', () => {
    it('should return null for valid ExecuteResult', () => {
      expect(validateExecuteResult({ affected: 0 })).toBeNull();
      expect(validateExecuteResult({ affected: 5, lastInsertId: '123' })).toBeNull();
    });

    it('should return error message for missing affected', () => {
      const error = validateExecuteResult({});
      expect(error).toContain('affected');
    });

    it('should return error message for negative affected', () => {
      const error = validateExecuteResult({ affected: -1 });
      expect(error).toContain('affected');
    });

    it('should return error message for invalid lastInsertId type', () => {
      const error = validateExecuteResult({ affected: 1, lastInsertId: true });
      expect(error).toContain('lastInsertId');
    });
  });
});
