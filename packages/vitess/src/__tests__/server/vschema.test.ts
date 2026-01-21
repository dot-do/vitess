/**
 * VSchema Tests - VSchema Parsing and Validation
 *
 * TDD Red tests for VSchema parsing, validation, and query.
 * VSchema defines how tables are sharded and how queries are routed.
 */

import { describe, it, expect } from 'vitest';
import {
  parseVSchema,
  validateVSchema,
  getTableVSchema,
  getPrimaryVindex,
  isSharded,
  getShards,
  createVSchemaBuilder,
  type VSchema,
  type KeyspaceVSchema,
  type TableVSchema,
  type VindexDefinition,
  type VSchemaValidationResult,
} from '../../server/vschema.js';

describe('VSchema Parsing', () => {
  describe('parseVSchema()', () => {
    it('should parse valid JSON VSchema', () => {
      const json = {
        keyspaces: {
          commerce: {
            sharded: true,
            vindexes: {
              hash: { type: 'hash' },
            },
            tables: {
              users: {
                column_vindexes: [{ column: 'id', name: 'hash' }],
              },
            },
            shards: ['-80', '80-'],
          },
        },
      };

      const vschema = parseVSchema(json);

      expect(vschema.keyspaces).toBeDefined();
      expect(vschema.keyspaces.commerce).toBeDefined();
      expect(vschema.keyspaces.commerce.sharded).toBe(true);
    });

    it('should parse VSchema from JSON string', () => {
      const jsonStr = JSON.stringify({
        keyspaces: {
          main: {
            sharded: false,
            tables: { settings: {} },
          },
        },
      });

      const vschema = parseVSchema(jsonStr);

      expect(vschema.keyspaces.main).toBeDefined();
      expect(vschema.keyspaces.main.sharded).toBe(false);
    });

    it('should parse empty VSchema', () => {
      const vschema = parseVSchema({ keyspaces: {} });

      expect(vschema.keyspaces).toEqual({});
    });

    it('should parse VSchema with multiple keyspaces', () => {
      const vschema = parseVSchema({
        keyspaces: {
          commerce: { sharded: true, tables: {} },
          lookup: { sharded: false, tables: {} },
          analytics: { sharded: true, tables: {} },
        },
      });

      expect(Object.keys(vschema.keyspaces)).toHaveLength(3);
    });

    it('should parse VSchema with all vindex types', () => {
      const vschema = parseVSchema({
        keyspaces: {
          test: {
            sharded: true,
            vindexes: {
              hash_idx: { type: 'hash' },
              consistent_idx: { type: 'consistent_hash' },
              range_idx: { type: 'range' },
              lookup_idx: { type: 'lookup', table: 'lookup_tbl' },
            },
            tables: {},
          },
        },
      });

      expect(vschema.keyspaces.test.vindexes?.hash_idx.type).toBe('hash');
      expect(vschema.keyspaces.test.vindexes?.consistent_idx.type).toBe('consistent_hash');
      expect(vschema.keyspaces.test.vindexes?.range_idx.type).toBe('range');
      expect(vschema.keyspaces.test.vindexes?.lookup_idx.type).toBe('lookup');
    });

    it('should parse VSchema with auto_increment configuration', () => {
      const vschema = parseVSchema({
        keyspaces: {
          test: {
            sharded: true,
            tables: {
              users: {
                column_vindexes: [{ column: 'id', name: 'hash' }],
                auto_increment: {
                  column: 'id',
                  sequence: 'users_seq',
                },
              },
            },
          },
        },
      });

      expect(vschema.keyspaces.test.tables.users.auto_increment).toBeDefined();
      expect(vschema.keyspaces.test.tables.users.auto_increment?.column).toBe('id');
      expect(vschema.keyspaces.test.tables.users.auto_increment?.sequence).toBe('users_seq');
    });

    it('should parse VSchema with sequence tables', () => {
      const vschema = parseVSchema({
        keyspaces: {
          sequences: {
            sharded: false,
            tables: {
              users_seq: { type: 'sequence' },
              orders_seq: { type: 'sequence' },
            },
          },
        },
      });

      expect(vschema.keyspaces.sequences.tables.users_seq.type).toBe('sequence');
    });

    it('should parse VSchema with reference tables', () => {
      const vschema = parseVSchema({
        keyspaces: {
          commerce: {
            sharded: true,
            tables: {
              countries: { type: 'reference' },
            },
          },
        },
      });

      expect(vschema.keyspaces.commerce.tables.countries.type).toBe('reference');
    });

    it('should throw error for invalid JSON', () => {
      expect(() => parseVSchema('not valid json {')).toThrow();
    });

    it('should throw error for missing keyspaces field', () => {
      expect(() => parseVSchema({ tables: {} })).toThrow(/keyspaces/i);
    });
  });

  describe('validateVSchema()', () => {
    it('should validate correct VSchema', () => {
      const vschema: VSchema = {
        keyspaces: {
          commerce: {
            sharded: true,
            vindexes: { hash: { type: 'hash' } },
            tables: {
              users: {
                column_vindexes: [{ column: 'id', name: 'hash' }],
              },
            },
            shards: ['-80', '80-'],
          },
        },
      };

      const result = validateVSchema(vschema);

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should error when sharded keyspace has no vindexes', () => {
      const vschema: VSchema = {
        keyspaces: {
          commerce: {
            sharded: true,
            tables: {
              users: {},
            },
          },
        },
      };

      const result = validateVSchema(vschema);

      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.message.includes('vindex'))).toBe(true);
    });

    it('should error when table references non-existent vindex', () => {
      const vschema: VSchema = {
        keyspaces: {
          commerce: {
            sharded: true,
            vindexes: { hash: { type: 'hash' } },
            tables: {
              users: {
                column_vindexes: [{ column: 'id', name: 'nonexistent' }],
              },
            },
          },
        },
      };

      const result = validateVSchema(vschema);

      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.code === 'UNKNOWN_VINDEX')).toBe(true);
    });

    it('should error when sharded table has no primary vindex', () => {
      const vschema: VSchema = {
        keyspaces: {
          commerce: {
            sharded: true,
            vindexes: { hash: { type: 'hash' } },
            tables: {
              users: {},
            },
          },
        },
      };

      const result = validateVSchema(vschema);

      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.message.includes('primary vindex'))).toBe(true);
    });

    it('should warn when using non-unique vindex as primary', () => {
      const vschema: VSchema = {
        keyspaces: {
          commerce: {
            sharded: true,
            vindexes: {
              lookup: { type: 'lookup', table: 'lookup_tbl' },
            },
            tables: {
              users: {
                column_vindexes: [{ column: 'email', name: 'lookup' }],
              },
            },
          },
        },
      };

      const result = validateVSchema(vschema);

      expect(result.warnings.length).toBeGreaterThan(0);
    });

    it('should error when lookup vindex has no table specified', () => {
      const vschema: VSchema = {
        keyspaces: {
          commerce: {
            sharded: true,
            vindexes: {
              lookup: { type: 'lookup' }, // missing table
            },
            tables: {},
          },
        },
      };

      const result = validateVSchema(vschema);

      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.message.includes('lookup table'))).toBe(true);
    });

    it('should error when sequence references non-existent sequence table', () => {
      const vschema: VSchema = {
        keyspaces: {
          commerce: {
            sharded: true,
            vindexes: { hash: { type: 'hash' } },
            tables: {
              users: {
                column_vindexes: [{ column: 'id', name: 'hash' }],
                auto_increment: {
                  column: 'id',
                  sequence: 'nonexistent_seq',
                },
              },
            },
          },
        },
      };

      const result = validateVSchema(vschema);

      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.message.includes('sequence'))).toBe(true);
    });

    it('should validate shard range format', () => {
      const vschema: VSchema = {
        keyspaces: {
          commerce: {
            sharded: true,
            vindexes: { hash: { type: 'hash' } },
            tables: { users: { column_vindexes: [{ column: 'id', name: 'hash' }] } },
            shards: ['invalid-shard-format'],
          },
        },
      };

      const result = validateVSchema(vschema);

      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.message.includes('shard'))).toBe(true);
    });

    it('should validate that shards cover full keyspace range', () => {
      const vschema: VSchema = {
        keyspaces: {
          commerce: {
            sharded: true,
            vindexes: { hash: { type: 'hash' } },
            tables: { users: { column_vindexes: [{ column: 'id', name: 'hash' }] } },
            shards: ['-40', '80-'], // Missing 40-80
          },
        },
      };

      const result = validateVSchema(vschema);

      expect(result.warnings.some((w) => w.message.includes('gap'))).toBe(true);
    });
  });

  describe('getTableVSchema()', () => {
    const vschema: VSchema = {
      keyspaces: {
        commerce: {
          sharded: true,
          vindexes: { hash: { type: 'hash' } },
          tables: {
            users: { column_vindexes: [{ column: 'id', name: 'hash' }] },
            orders: { column_vindexes: [{ column: 'user_id', name: 'hash' }] },
          },
        },
      },
    };

    it('should return table configuration', () => {
      const table = getTableVSchema(vschema, 'commerce', 'users');

      expect(table).toBeDefined();
      expect(table?.column_vindexes).toHaveLength(1);
    });

    it('should return undefined for non-existent table', () => {
      const table = getTableVSchema(vschema, 'commerce', 'nonexistent');

      expect(table).toBeUndefined();
    });

    it('should return undefined for non-existent keyspace', () => {
      const table = getTableVSchema(vschema, 'nonexistent', 'users');

      expect(table).toBeUndefined();
    });
  });

  describe('getPrimaryVindex()', () => {
    const vschema: VSchema = {
      keyspaces: {
        commerce: {
          sharded: true,
          vindexes: {
            hash: { type: 'hash' },
            email_lookup: { type: 'lookup_unique', table: 'email_to_user' },
          },
          tables: {
            users: {
              column_vindexes: [
                { column: 'id', name: 'hash' },
                { column: 'email', name: 'email_lookup' },
              ],
            },
          },
        },
      },
    };

    it('should return primary vindex (first in list)', () => {
      const vindex = getPrimaryVindex(vschema, 'commerce', 'users');

      expect(vindex).toBeDefined();
      expect(vindex?.type).toBe('hash');
    });

    it('should return undefined for table without vindexes', () => {
      const noVindexSchema: VSchema = {
        keyspaces: {
          lookup: {
            sharded: false,
            tables: { countries: {} },
          },
        },
      };

      const vindex = getPrimaryVindex(noVindexSchema, 'lookup', 'countries');

      expect(vindex).toBeUndefined();
    });
  });

  describe('isSharded()', () => {
    const vschema: VSchema = {
      keyspaces: {
        sharded_ks: { sharded: true, tables: {} },
        unsharded_ks: { sharded: false, tables: {} },
      },
    };

    it('should return true for sharded keyspace', () => {
      expect(isSharded(vschema, 'sharded_ks')).toBe(true);
    });

    it('should return false for unsharded keyspace', () => {
      expect(isSharded(vschema, 'unsharded_ks')).toBe(false);
    });

    it('should throw for non-existent keyspace', () => {
      expect(() => isSharded(vschema, 'nonexistent')).toThrow();
    });
  });

  describe('getShards()', () => {
    const vschema: VSchema = {
      keyspaces: {
        commerce: {
          sharded: true,
          tables: {},
          shards: ['-40', '40-80', '80-c0', 'c0-'],
        },
        unsharded: {
          sharded: false,
          tables: {},
        },
      },
    };

    it('should return shard list for sharded keyspace', () => {
      const shards = getShards(vschema, 'commerce');

      expect(shards).toHaveLength(4);
      expect(shards).toContain('-40');
      expect(shards).toContain('c0-');
    });

    it('should return single shard for unsharded keyspace', () => {
      const shards = getShards(vschema, 'unsharded');

      expect(shards).toHaveLength(1);
      expect(shards).toContain('-');
    });
  });
});

describe('VSchemaBuilder', () => {
  describe('addKeyspace()', () => {
    it('should add sharded keyspace', () => {
      const vschema = createVSchemaBuilder()
        .addKeyspace('commerce', true)
        .build();

      expect(vschema.keyspaces.commerce).toBeDefined();
      expect(vschema.keyspaces.commerce.sharded).toBe(true);
    });

    it('should add unsharded keyspace', () => {
      const vschema = createVSchemaBuilder()
        .addKeyspace('lookup', false)
        .build();

      expect(vschema.keyspaces.lookup.sharded).toBe(false);
    });

    it('should support method chaining', () => {
      const builder = createVSchemaBuilder()
        .addKeyspace('commerce', true)
        .addKeyspace('lookup', false);

      expect(builder.build().keyspaces).toHaveProperty('commerce');
      expect(builder.build().keyspaces).toHaveProperty('lookup');
    });
  });

  describe('addVindex()', () => {
    it('should add vindex to keyspace', () => {
      const vschema = createVSchemaBuilder()
        .addKeyspace('commerce', true)
        .addVindex('commerce', 'hash', { type: 'hash' })
        .build();

      expect(vschema.keyspaces.commerce.vindexes?.hash).toBeDefined();
      expect(vschema.keyspaces.commerce.vindexes?.hash.type).toBe('hash');
    });

    it('should add vindex with params', () => {
      const vschema = createVSchemaBuilder()
        .addKeyspace('test', true)
        .addVindex('test', 'xxhash', {
          type: 'hash',
          params: { hash_function: 'xxhash' },
        })
        .build();

      expect(vschema.keyspaces.test.vindexes?.xxhash.params?.hash_function).toBe('xxhash');
    });

    it('should throw when adding vindex to non-existent keyspace', () => {
      expect(() => {
        createVSchemaBuilder().addVindex('nonexistent', 'hash', { type: 'hash' });
      }).toThrow(/keyspace.*not found/i);
    });
  });

  describe('addTable()', () => {
    it('should add table to keyspace', () => {
      const vschema = createVSchemaBuilder()
        .addKeyspace('commerce', true)
        .addVindex('commerce', 'hash', { type: 'hash' })
        .addTable('commerce', 'users', {
          column_vindexes: [{ column: 'id', name: 'hash' }],
        })
        .build();

      expect(vschema.keyspaces.commerce.tables.users).toBeDefined();
    });

    it('should add table with auto_increment', () => {
      const vschema = createVSchemaBuilder()
        .addKeyspace('commerce', true)
        .addVindex('commerce', 'hash', { type: 'hash' })
        .addTable('commerce', 'users', {
          column_vindexes: [{ column: 'id', name: 'hash' }],
          auto_increment: { column: 'id', sequence: 'users_seq' },
        })
        .build();

      expect(vschema.keyspaces.commerce.tables.users.auto_increment).toBeDefined();
    });
  });

  describe('setShards()', () => {
    it('should set shards for keyspace', () => {
      const vschema = createVSchemaBuilder()
        .addKeyspace('commerce', true)
        .setShards('commerce', ['-80', '80-'])
        .build();

      expect(vschema.keyspaces.commerce.shards).toEqual(['-80', '80-']);
    });

    it('should support 4-way sharding', () => {
      const vschema = createVSchemaBuilder()
        .addKeyspace('commerce', true)
        .setShards('commerce', ['-40', '40-80', '80-c0', 'c0-'])
        .build();

      expect(vschema.keyspaces.commerce.shards).toHaveLength(4);
    });
  });

  describe('Full VSchema construction', () => {
    it('should build complete VSchema', () => {
      const vschema = createVSchemaBuilder()
        .addKeyspace('commerce', true)
        .addVindex('commerce', 'hash', { type: 'hash' })
        .addVindex('commerce', 'email_lookup', {
          type: 'lookup_unique',
          table: 'email_to_user',
          from: ['email'],
          to: 'user_id',
        })
        .addTable('commerce', 'users', {
          column_vindexes: [
            { column: 'id', name: 'hash' },
            { column: 'email', name: 'email_lookup' },
          ],
          auto_increment: { column: 'id', sequence: 'users_seq' },
        })
        .addTable('commerce', 'orders', {
          column_vindexes: [{ column: 'user_id', name: 'hash' }],
        })
        .setShards('commerce', ['-80', '80-'])
        .addKeyspace('lookup', false)
        .addTable('lookup', 'email_to_user', {})
        .addTable('lookup', 'countries', { type: 'reference' })
        .addKeyspace('sequences', false)
        .addTable('sequences', 'users_seq', { type: 'sequence' })
        .build();

      // Validate the built VSchema
      const result = validateVSchema(vschema);
      expect(result.valid).toBe(true);

      // Check structure
      expect(vschema.keyspaces.commerce.sharded).toBe(true);
      expect(vschema.keyspaces.lookup.sharded).toBe(false);
      expect(vschema.keyspaces.sequences.tables.users_seq.type).toBe('sequence');
    });
  });
});
