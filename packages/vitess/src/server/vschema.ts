/**
 * VSchema - Virtual Schema Configuration
 *
 * Defines how tables are sharded and how queries are routed.
 * Key concepts:
 * - Keyspace: Logical database (can be sharded or unsharded)
 * - Vindex: Virtual index for shard key computation
 * - Table: Table definition with primary vindex and optional secondary vindexes
 */

import type { VindexType, VindexParams } from './vindexes.js';

/**
 * Complete VSchema for all keyspaces
 */
export interface VSchema {
  keyspaces: Record<string, KeyspaceVSchema>;
}

/**
 * Keyspace VSchema
 */
export interface KeyspaceVSchema {
  /** Whether this keyspace is sharded */
  sharded: boolean;
  /** Vindex definitions for this keyspace */
  vindexes?: Record<string, VindexDefinition>;
  /** Table definitions */
  tables: Record<string, TableVSchema>;
  /** Shard definitions (for sharded keyspaces) */
  shards?: string[];
}

/**
 * Vindex definition in VSchema
 */
export interface VindexDefinition {
  /** Vindex type (hash, consistent_hash, range, lookup) */
  type: VindexType;
  /** Optional parameters */
  params?: VindexParams;
  /** For lookup vindexes: the lookup table */
  table?: string;
  /** For lookup vindexes: columns mapping */
  columns?: string[];
}

/**
 * Table VSchema definition
 */
export interface TableVSchema {
  /** Column vindexes - maps column names to vindex names */
  column_vindexes?: ColumnVindex[];
  /** Auto-increment column configuration */
  auto_increment?: AutoIncrementConfig;
  /** Table type */
  type?: 'sequence' | 'reference' | '';
  /** Pinned tablet type for reads */
  pinned?: string;
}

/**
 * Column vindex mapping
 */
export interface ColumnVindex {
  /** Column name(s) */
  column?: string;
  columns?: string[];
  /** Vindex name (references vindexes in keyspace) */
  name: string;
}

/**
 * Auto-increment configuration
 */
export interface AutoIncrementConfig {
  column: string;
  sequence: string;
}

/**
 * VSchema validation result
 */
export interface VSchemaValidationResult {
  valid: boolean;
  errors: VSchemaError[];
  warnings: VSchemaWarning[];
}

/**
 * VSchema validation error
 */
export interface VSchemaError {
  keyspace?: string;
  table?: string;
  vindex?: string;
  message: string;
  code: string;
}

/**
 * VSchema validation warning
 */
export interface VSchemaWarning {
  keyspace?: string;
  table?: string;
  message: string;
}

/**
 * Parse VSchema from JSON
 */
export function parseVSchema(json: string | object): VSchema {
  // TODO: Implement VSchema parsing
  throw new Error('Not implemented');
}

/**
 * Validate VSchema configuration
 */
export function validateVSchema(vschema: VSchema): VSchemaValidationResult {
  // TODO: Implement VSchema validation
  throw new Error('Not implemented');
}

/**
 * Get table configuration from VSchema
 */
export function getTableVSchema(
  vschema: VSchema,
  keyspace: string,
  table: string
): TableVSchema | undefined {
  // TODO: Implement table lookup
  throw new Error('Not implemented');
}

/**
 * Get primary vindex for a table
 */
export function getPrimaryVindex(
  vschema: VSchema,
  keyspace: string,
  table: string
): VindexDefinition | undefined {
  // TODO: Implement primary vindex lookup
  throw new Error('Not implemented');
}

/**
 * Check if keyspace is sharded
 */
export function isSharded(vschema: VSchema, keyspace: string): boolean {
  // TODO: Implement sharded check
  throw new Error('Not implemented');
}

/**
 * Get all shards for a keyspace
 */
export function getShards(vschema: VSchema, keyspace: string): string[] {
  // TODO: Implement shard list
  throw new Error('Not implemented');
}

/**
 * VSchema builder for programmatic construction
 */
export class VSchemaBuilder {
  private vschema: VSchema = { keyspaces: {} };

  /**
   * Add a keyspace
   */
  addKeyspace(name: string, sharded: boolean = false): this {
    this.vschema.keyspaces[name] = {
      sharded,
      tables: {},
      vindexes: {},
    };
    return this;
  }

  /**
   * Add a vindex to a keyspace
   */
  addVindex(keyspace: string, name: string, definition: VindexDefinition): this {
    // TODO: Implement vindex addition
    throw new Error('Not implemented');
  }

  /**
   * Add a table to a keyspace
   */
  addTable(keyspace: string, name: string, config: TableVSchema): this {
    // TODO: Implement table addition
    throw new Error('Not implemented');
  }

  /**
   * Set shards for a keyspace
   */
  setShards(keyspace: string, shards: string[]): this {
    // TODO: Implement shard setting
    throw new Error('Not implemented');
  }

  /**
   * Build the VSchema
   */
  build(): VSchema {
    return this.vschema;
  }
}

/**
 * Create a VSchema builder
 */
export function createVSchemaBuilder(): VSchemaBuilder {
  return new VSchemaBuilder();
}
