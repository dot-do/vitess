/**
 * @dotdo/vitess-rpc - VSchema Test Suite (TDD Red)
 *
 * Tests for VSchema, Keyspace, VindexDef, TableDef validation.
 * These tests define expected behavior - implementations to be added.
 */

import { describe, it, expect } from 'vitest';

// These will need to be implemented
import {
  isKeyspace,
  isVindexType,
  isVindexDef,
  isTableDef,
  isVSchema,
  isStorageEngineType,
  isIsolationLevel,
  isTransactionOptions,
  isQueryTarget,
  isAggregateFunction,
  isAggregateSpec,
  validateKeyspace,
  validateVindexDef,
  validateTableDef,
  validateVSchema,
} from '../types.js';

describe('vschema.ts', () => {
  describe('StorageEngineType type guard', () => {
    it('should return true for valid engine types', () => {
      expect(isStorageEngineType('postgres')).toBe(true);
      expect(isStorageEngineType('sqlite')).toBe(true);
    });

    it('should return false for invalid engine types', () => {
      expect(isStorageEngineType('mysql')).toBe(false);
      expect(isStorageEngineType('oracle')).toBe(false);
      expect(isStorageEngineType('')).toBe(false);
      expect(isStorageEngineType(null)).toBe(false);
      expect(isStorageEngineType(123)).toBe(false);
    });
  });

  describe('Keyspace type guard', () => {
    it('should return true for valid Keyspace objects', () => {
      expect(isKeyspace({
        name: 'main',
        shardCount: 4,
        engine: 'postgres',
      })).toBe(true);
      expect(isKeyspace({
        name: 'test',
        shardCount: 1,
        engine: 'sqlite',
      })).toBe(true);
    });

    it('should return false for invalid Keyspace objects', () => {
      expect(isKeyspace({})).toBe(false);
      expect(isKeyspace({ name: 'main' })).toBe(false);
      expect(isKeyspace({ name: 'main', shardCount: 4 })).toBe(false);
      expect(isKeyspace({
        name: 'main',
        shardCount: 0, // must be >= 1
        engine: 'postgres',
      })).toBe(false);
      expect(isKeyspace({
        name: 'main',
        shardCount: -1,
        engine: 'postgres',
      })).toBe(false);
      expect(isKeyspace({
        name: '',
        shardCount: 4,
        engine: 'postgres',
      })).toBe(false);
      expect(isKeyspace({
        name: 'main',
        shardCount: 4,
        engine: 'invalid',
      })).toBe(false);
      expect(isKeyspace(null)).toBe(false);
    });
  });

  describe('VindexType type guard', () => {
    it('should return true for valid vindex types', () => {
      expect(isVindexType('hash')).toBe(true);
      expect(isVindexType('consistent_hash')).toBe(true);
      expect(isVindexType('range')).toBe(true);
      expect(isVindexType('lookup')).toBe(true);
      expect(isVindexType('null')).toBe(true);
    });

    it('should return false for invalid vindex types', () => {
      expect(isVindexType('invalid')).toBe(false);
      expect(isVindexType('')).toBe(false);
      expect(isVindexType(null)).toBe(false);
      expect(isVindexType(123)).toBe(false);
    });
  });

  describe('VindexDef type guard', () => {
    it('should return true for valid VindexDef objects', () => {
      expect(isVindexDef({
        type: 'hash',
        columns: ['id'],
      })).toBe(true);
      expect(isVindexDef({
        type: 'consistent_hash',
        columns: ['user_id', 'tenant_id'],
      })).toBe(true);
      expect(isVindexDef({
        type: 'lookup',
        columns: ['email'],
        lookupTable: 'email_lookup',
      })).toBe(true);
    });

    it('should return false for invalid VindexDef objects', () => {
      expect(isVindexDef({})).toBe(false);
      expect(isVindexDef({ type: 'hash' })).toBe(false);
      expect(isVindexDef({ columns: ['id'] })).toBe(false);
      expect(isVindexDef({
        type: 'invalid',
        columns: ['id'],
      })).toBe(false);
      expect(isVindexDef({
        type: 'hash',
        columns: [], // must have at least one column
      })).toBe(false);
      expect(isVindexDef({
        type: 'hash',
        columns: 'id', // must be array
      })).toBe(false);
      expect(isVindexDef(null)).toBe(false);
    });

    it('should validate lookupTable is string if present', () => {
      expect(isVindexDef({
        type: 'lookup',
        columns: ['email'],
        lookupTable: 123, // should be string
      })).toBe(false);
    });
  });

  describe('TableDef type guard', () => {
    it('should return true for valid TableDef objects', () => {
      expect(isTableDef({
        vindex: {
          type: 'hash',
          columns: ['id'],
        },
      })).toBe(true);
      expect(isTableDef({
        vindex: {
          type: 'consistent_hash',
          columns: ['user_id'],
        },
        autoIncrement: {
          column: 'id',
          sequence: 'users_id_seq',
        },
      })).toBe(true);
    });

    it('should return false for invalid TableDef objects', () => {
      expect(isTableDef({})).toBe(false);
      expect(isTableDef({ vindex: {} })).toBe(false);
      expect(isTableDef({
        vindex: {
          type: 'invalid',
          columns: ['id'],
        },
      })).toBe(false);
      expect(isTableDef(null)).toBe(false);
    });

    it('should validate autoIncrement if present', () => {
      expect(isTableDef({
        vindex: { type: 'hash', columns: ['id'] },
        autoIncrement: {}, // missing required fields
      })).toBe(false);
      expect(isTableDef({
        vindex: { type: 'hash', columns: ['id'] },
        autoIncrement: { column: 'id' }, // missing sequence
      })).toBe(false);
      expect(isTableDef({
        vindex: { type: 'hash', columns: ['id'] },
        autoIncrement: { column: 123, sequence: 'seq' }, // column must be string
      })).toBe(false);
    });
  });

  describe('VSchema type guard', () => {
    it('should return true for valid VSchema objects', () => {
      expect(isVSchema({
        keyspace: 'main',
        sharded: false,
        tables: {},
        vindexes: {},
      })).toBe(true);
      expect(isVSchema({
        keyspace: 'commerce',
        sharded: true,
        tables: {
          users: {
            vindex: { type: 'hash', columns: ['id'] },
          },
        },
        vindexes: {
          user_hash: { type: 'hash', columns: ['id'] },
        },
      })).toBe(true);
    });

    it('should return false for invalid VSchema objects', () => {
      expect(isVSchema({})).toBe(false);
      expect(isVSchema({ keyspace: 'main' })).toBe(false);
      expect(isVSchema({
        keyspace: '',
        sharded: false,
        tables: {},
        vindexes: {},
      })).toBe(false);
      expect(isVSchema({
        keyspace: 'main',
        sharded: 'yes', // must be boolean
        tables: {},
        vindexes: {},
      })).toBe(false);
      expect(isVSchema({
        keyspace: 'main',
        sharded: false,
        tables: 'invalid',
        vindexes: {},
      })).toBe(false);
      expect(isVSchema(null)).toBe(false);
    });

    it('should validate nested tables and vindexes', () => {
      expect(isVSchema({
        keyspace: 'main',
        sharded: true,
        tables: {
          users: { invalid: 'table' },
        },
        vindexes: {},
      })).toBe(false);
      expect(isVSchema({
        keyspace: 'main',
        sharded: true,
        tables: {},
        vindexes: {
          invalid_vindex: { columns: [] }, // missing type
        },
      })).toBe(false);
    });
  });

  describe('IsolationLevel type guard', () => {
    it('should return true for valid isolation levels', () => {
      expect(isIsolationLevel('read_uncommitted')).toBe(true);
      expect(isIsolationLevel('read_committed')).toBe(true);
      expect(isIsolationLevel('repeatable_read')).toBe(true);
      expect(isIsolationLevel('serializable')).toBe(true);
    });

    it('should return false for invalid isolation levels', () => {
      expect(isIsolationLevel('invalid')).toBe(false);
      expect(isIsolationLevel('')).toBe(false);
      expect(isIsolationLevel(null)).toBe(false);
    });
  });

  describe('TransactionOptions type guard', () => {
    it('should return true for valid TransactionOptions', () => {
      expect(isTransactionOptions({})).toBe(true);
      expect(isTransactionOptions({ isolation: 'serializable' })).toBe(true);
      expect(isTransactionOptions({ readOnly: true })).toBe(true);
      expect(isTransactionOptions({ timeout: 5000 })).toBe(true);
      expect(isTransactionOptions({
        isolation: 'read_committed',
        readOnly: false,
        timeout: 10000,
      })).toBe(true);
    });

    it('should return false for invalid TransactionOptions', () => {
      expect(isTransactionOptions({ isolation: 'invalid' })).toBe(false);
      expect(isTransactionOptions({ readOnly: 'yes' })).toBe(false);
      expect(isTransactionOptions({ timeout: -1 })).toBe(false);
      expect(isTransactionOptions({ timeout: 'fast' })).toBe(false);
      expect(isTransactionOptions(null)).toBe(false);
    });
  });

  describe('QueryTarget type guard', () => {
    it('should return true for valid QueryTarget objects', () => {
      expect(isQueryTarget({ keyspace: 'main' })).toBe(true);
      expect(isQueryTarget({ keyspace: 'main', shard: 'shard-0' })).toBe(true);
      expect(isQueryTarget({ keyspace: 'main', scatter: true })).toBe(true);
    });

    it('should return false for invalid QueryTarget objects', () => {
      expect(isQueryTarget({})).toBe(false);
      expect(isQueryTarget({ keyspace: '' })).toBe(false);
      expect(isQueryTarget({ keyspace: 123 })).toBe(false);
      expect(isQueryTarget({ keyspace: 'main', scatter: 'yes' })).toBe(false);
      expect(isQueryTarget(null)).toBe(false);
    });
  });

  describe('AggregateFunction type guard', () => {
    it('should return true for valid aggregate functions', () => {
      expect(isAggregateFunction('COUNT')).toBe(true);
      expect(isAggregateFunction('SUM')).toBe(true);
      expect(isAggregateFunction('AVG')).toBe(true);
      expect(isAggregateFunction('MIN')).toBe(true);
      expect(isAggregateFunction('MAX')).toBe(true);
    });

    it('should return false for invalid aggregate functions', () => {
      expect(isAggregateFunction('INVALID')).toBe(false);
      expect(isAggregateFunction('count')).toBe(false); // case sensitive
      expect(isAggregateFunction('')).toBe(false);
      expect(isAggregateFunction(null)).toBe(false);
    });
  });

  describe('AggregateSpec type guard', () => {
    it('should return true for valid AggregateSpec objects', () => {
      expect(isAggregateSpec({
        function: 'COUNT',
        column: '*',
      })).toBe(true);
      expect(isAggregateSpec({
        function: 'SUM',
        column: 'amount',
        alias: 'total_amount',
      })).toBe(true);
    });

    it('should return false for invalid AggregateSpec objects', () => {
      expect(isAggregateSpec({})).toBe(false);
      expect(isAggregateSpec({ function: 'COUNT' })).toBe(false);
      expect(isAggregateSpec({ column: '*' })).toBe(false);
      expect(isAggregateSpec({
        function: 'INVALID',
        column: '*',
      })).toBe(false);
      expect(isAggregateSpec({
        function: 'COUNT',
        column: '',
      })).toBe(false);
      expect(isAggregateSpec(null)).toBe(false);
    });
  });

  describe('validateKeyspace', () => {
    it('should return null for valid Keyspace', () => {
      expect(validateKeyspace({
        name: 'main',
        shardCount: 4,
        engine: 'postgres',
      })).toBeNull();
    });

    it('should return error for missing name', () => {
      const error = validateKeyspace({ shardCount: 4, engine: 'postgres' });
      expect(error).toContain('name');
    });

    it('should return error for empty name', () => {
      const error = validateKeyspace({ name: '', shardCount: 4, engine: 'postgres' });
      expect(error).toContain('name');
    });

    it('should return error for invalid shardCount', () => {
      const error = validateKeyspace({ name: 'main', shardCount: 0, engine: 'postgres' });
      expect(error).toContain('shardCount');
    });

    it('should return error for invalid engine', () => {
      const error = validateKeyspace({ name: 'main', shardCount: 4, engine: 'mysql' });
      expect(error).toContain('engine');
    });
  });

  describe('validateVindexDef', () => {
    it('should return null for valid VindexDef', () => {
      expect(validateVindexDef({
        type: 'hash',
        columns: ['id'],
      })).toBeNull();
    });

    it('should return error for missing type', () => {
      const error = validateVindexDef({ columns: ['id'] });
      expect(error).toContain('type');
    });

    it('should return error for invalid type', () => {
      const error = validateVindexDef({ type: 'invalid', columns: ['id'] });
      expect(error).toContain('type');
    });

    it('should return error for missing columns', () => {
      const error = validateVindexDef({ type: 'hash' });
      expect(error).toContain('columns');
    });

    it('should return error for empty columns array', () => {
      const error = validateVindexDef({ type: 'hash', columns: [] });
      expect(error).toContain('columns');
    });
  });

  describe('validateTableDef', () => {
    it('should return null for valid TableDef', () => {
      expect(validateTableDef({
        vindex: { type: 'hash', columns: ['id'] },
      })).toBeNull();
    });

    it('should return error for missing vindex', () => {
      const error = validateTableDef({});
      expect(error).toContain('vindex');
    });

    it('should return error for invalid vindex', () => {
      const error = validateTableDef({ vindex: {} });
      expect(error).toContain('vindex');
    });

    it('should return error for invalid autoIncrement', () => {
      const error = validateTableDef({
        vindex: { type: 'hash', columns: ['id'] },
        autoIncrement: { column: 'id' }, // missing sequence
      });
      expect(error).toContain('autoIncrement');
    });
  });

  describe('validateVSchema', () => {
    it('should return null for valid VSchema', () => {
      expect(validateVSchema({
        keyspace: 'main',
        sharded: false,
        tables: {},
        vindexes: {},
      })).toBeNull();
    });

    it('should return error for missing keyspace', () => {
      const error = validateVSchema({
        sharded: false,
        tables: {},
        vindexes: {},
      });
      expect(error).toContain('keyspace');
    });

    it('should return error for missing sharded flag', () => {
      const error = validateVSchema({
        keyspace: 'main',
        tables: {},
        vindexes: {},
      });
      expect(error).toContain('sharded');
    });

    it('should return error for invalid table definition', () => {
      const error = validateVSchema({
        keyspace: 'main',
        sharded: true,
        tables: {
          users: { invalid: 'def' },
        },
        vindexes: {},
      });
      expect(error).toContain('tables');
    });

    it('should return error for invalid vindex definition', () => {
      const error = validateVSchema({
        keyspace: 'main',
        sharded: true,
        tables: {},
        vindexes: {
          bad_vindex: { columns: [] },
        },
      });
      expect(error).toContain('vindexes');
    });
  });
});
