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
  /** For lookup vindexes: from columns */
  from?: string[];
  /** For lookup vindexes: to column */
  to?: string;
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
  let parsed: unknown;

  if (typeof json === 'string') {
    try {
      parsed = JSON.parse(json);
    } catch (e) {
      throw new Error(`Invalid JSON: ${(e as Error).message}`);
    }
  } else {
    parsed = json;
  }

  if (!parsed || typeof parsed !== 'object') {
    throw new Error('VSchema must be an object');
  }

  const obj = parsed as Record<string, unknown>;

  if (!('keyspaces' in obj)) {
    throw new Error('VSchema must have a keyspaces field');
  }

  const keyspaces = obj.keyspaces as Record<string, KeyspaceVSchema>;

  return { keyspaces };
}

/**
 * Validate VSchema configuration
 */
export function validateVSchema(vschema: VSchema): VSchemaValidationResult {
  const errors: VSchemaError[] = [];
  const warnings: VSchemaWarning[] = [];

  for (const [keyspaceName, keyspace] of Object.entries(vschema.keyspaces)) {
    // Check if sharded keyspace has vindexes
    if (keyspace.sharded && (!keyspace.vindexes || Object.keys(keyspace.vindexes).length === 0)) {
      errors.push({
        keyspace: keyspaceName,
        message: 'Sharded keyspace must have at least one vindex defined',
        code: 'MISSING_VINDEX',
      });
    }

    // Check lookup vindexes have table specified
    if (keyspace.vindexes) {
      for (const [vindexName, vindex] of Object.entries(keyspace.vindexes)) {
        if ((vindex.type === 'lookup' || vindex.type === 'lookup_unique' || vindex.type === 'lookup_hash') && !vindex.table) {
          errors.push({
            keyspace: keyspaceName,
            vindex: vindexName,
            message: 'Lookup vindex must have a lookup table specified',
            code: 'MISSING_LOOKUP_TABLE',
          });
        }
      }
    }

    // Check tables
    for (const [tableName, table] of Object.entries(keyspace.tables)) {
      // Check if sharded table has primary vindex
      if (keyspace.sharded && table.type !== 'sequence' && table.type !== 'reference') {
        if (!table.column_vindexes || table.column_vindexes.length === 0) {
          errors.push({
            keyspace: keyspaceName,
            table: tableName,
            message: 'Sharded table must have a primary vindex defined',
            code: 'MISSING_PRIMARY_VINDEX',
          });
        }
      }

      // Check column vindexes reference existing vindexes
      if (table.column_vindexes) {
        for (const colVindex of table.column_vindexes) {
          if (keyspace.vindexes && !keyspace.vindexes[colVindex.name]) {
            errors.push({
              keyspace: keyspaceName,
              table: tableName,
              vindex: colVindex.name,
              message: `Table references unknown vindex: ${colVindex.name}`,
              code: 'UNKNOWN_VINDEX',
            });
          }

          // Warn if using non-unique vindex as primary
          if (keyspace.vindexes) {
            const vindex = keyspace.vindexes[colVindex.name];
            if (vindex && (vindex.type === 'lookup' || vindex.type === 'lookup_hash') && table.column_vindexes?.indexOf(colVindex) === 0) {
              warnings.push({
                keyspace: keyspaceName,
                table: tableName,
                message: `Using non-unique vindex '${colVindex.name}' as primary vindex may cause scatter queries`,
              });
            }
          }
        }
      }

      // Check auto_increment references existing sequence
      if (table.auto_increment) {
        const seqName = table.auto_increment.sequence;
        let sequenceFound = false;

        // Search for sequence table in all keyspaces
        for (const [ksName, ks] of Object.entries(vschema.keyspaces)) {
          for (const [tblName, tbl] of Object.entries(ks.tables)) {
            if (tblName === seqName && tbl.type === 'sequence') {
              sequenceFound = true;
              break;
            }
          }
          if (sequenceFound) break;
        }

        if (!sequenceFound) {
          errors.push({
            keyspace: keyspaceName,
            table: tableName,
            message: `Auto-increment references non-existent sequence: ${seqName}`,
            code: 'MISSING_SEQUENCE',
          });
        }
      }
    }

    // Validate shard ranges
    if (keyspace.shards) {
      for (const shard of keyspace.shards) {
        if (!isValidShardRange(shard)) {
          errors.push({
            keyspace: keyspaceName,
            message: `Invalid shard range format: ${shard}`,
            code: 'INVALID_SHARD_RANGE',
          });
        }
      }

      // Check for gaps in shard ranges
      if (keyspace.shards.every(s => isValidShardRange(s))) {
        const hasGaps = checkShardGaps(keyspace.shards);
        if (hasGaps) {
          warnings.push({
            keyspace: keyspaceName,
            message: 'Shard ranges have gaps - some keyspace IDs may not be routable',
          });
        }
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

/**
 * Check if a shard range is valid
 */
function isValidShardRange(shard: string): boolean {
  // Valid formats: '-', '-80', '80-', '40-80'
  if (shard === '-') return true;

  const parts = shard.split('-');
  if (parts.length !== 2) return false;

  const [start, end] = parts;

  // Check hex format
  if (start && !/^[0-9a-fA-F]+$/.test(start)) return false;
  if (end && !/^[0-9a-fA-F]+$/.test(end)) return false;

  // If both present, start must be less than end
  if (start && end) {
    const startVal = parseInt(start, 16);
    const endVal = parseInt(end, 16);
    if (startVal >= endVal) return false;
  }

  return true;
}

/**
 * Check if shard ranges have gaps
 */
function checkShardGaps(shards: string[]): boolean {
  if (shards.length === 0) return true;
  if (shards.length === 1 && shards[0] === '-') return false;

  // Parse and sort ranges
  const ranges: { start: number; end: number }[] = [];

  for (const shard of shards) {
    const parts = shard.split('-');
    const start = parts[0] ? parseInt(parts[0], 16) : 0;
    const end = parts[1] ? parseInt(parts[1], 16) : 0x100;
    ranges.push({ start, end });
  }

  ranges.sort((a, b) => a.start - b.start);

  // Check first range starts at 0
  if (ranges[0].start !== 0) return true;

  // Check consecutive ranges
  for (let i = 1; i < ranges.length; i++) {
    if (ranges[i].start !== ranges[i - 1].end) return true;
  }

  // Check last range ends at 0x100 (or higher for 64-bit)
  // We check for 0x100 as that's the normalized 8-bit boundary
  // In practice, the max depends on the keyspace ID size

  return false;
}

/**
 * Get table configuration from VSchema
 */
export function getTableVSchema(
  vschema: VSchema,
  keyspace: string,
  table: string
): TableVSchema | undefined {
  const ks = vschema.keyspaces[keyspace];
  if (!ks) return undefined;
  return ks.tables[table];
}

/**
 * Get primary vindex for a table
 */
export function getPrimaryVindex(
  vschema: VSchema,
  keyspace: string,
  table: string
): VindexDefinition | undefined {
  const ks = vschema.keyspaces[keyspace];
  if (!ks) return undefined;

  const tbl = ks.tables[table];
  if (!tbl || !tbl.column_vindexes || tbl.column_vindexes.length === 0) {
    return undefined;
  }

  const primaryVindexName = tbl.column_vindexes[0].name;
  return ks.vindexes?.[primaryVindexName];
}

/**
 * Check if keyspace is sharded
 */
export function isSharded(vschema: VSchema, keyspace: string): boolean {
  const ks = vschema.keyspaces[keyspace];
  if (!ks) {
    throw new Error(`Keyspace '${keyspace}' not found`);
  }
  return ks.sharded;
}

/**
 * Get all shards for a keyspace
 */
export function getShards(vschema: VSchema, keyspace: string): string[] {
  const ks = vschema.keyspaces[keyspace];
  if (!ks) {
    throw new Error(`Keyspace '${keyspace}' not found`);
  }

  // Unsharded keyspace has single '-' shard
  if (!ks.sharded) {
    return ['-'];
  }

  return ks.shards ?? [];
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
    const ks = this.vschema.keyspaces[keyspace];
    if (!ks) {
      throw new Error(`Keyspace '${keyspace}' not found - add keyspace first`);
    }

    if (!ks.vindexes) {
      ks.vindexes = {};
    }

    ks.vindexes[name] = definition;
    return this;
  }

  /**
   * Add a table to a keyspace
   */
  addTable(keyspace: string, name: string, config: TableVSchema): this {
    const ks = this.vschema.keyspaces[keyspace];
    if (!ks) {
      throw new Error(`Keyspace '${keyspace}' not found - add keyspace first`);
    }

    ks.tables[name] = config;
    return this;
  }

  /**
   * Set shards for a keyspace
   */
  setShards(keyspace: string, shards: string[]): this {
    const ks = this.vschema.keyspaces[keyspace];
    if (!ks) {
      throw new Error(`Keyspace '${keyspace}' not found - add keyspace first`);
    }

    ks.shards = shards;
    return this;
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
