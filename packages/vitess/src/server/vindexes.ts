/**
 * Vindexes - Virtual Indexes for Shard Key Computation
 *
 * Vindexes determine which shard a row belongs to.
 * Types:
 * - hash: MD5/murmur hash of column value
 * - consistent_hash: Consistent hashing for better rebalancing
 * - range: Numeric range-based sharding
 * - lookup: Secondary index lookup to find shard
 *
 * Vindexes can be:
 * - Unique: Guarantees single shard routing
 * - Non-unique: May require scatter queries
 * - Functional: Computed from column value
 * - Lookup: Requires secondary table lookup
 */

/**
 * Vindex types
 */
export type VindexType =
  | 'hash'            // Simple hash (MD5/murmur)
  | 'consistent_hash' // Consistent hashing (ketama-style)
  | 'range'           // Numeric range
  | 'lookup'          // Lookup table
  | 'lookup_hash'     // Lookup with hash
  | 'lookup_unique'   // Unique lookup
  | 'binary'          // Binary/raw bytes
  | 'binary_md5'      // MD5 of binary
  | 'numeric'         // Numeric passthrough
  | 'unicode_loose_md5'; // Unicode normalization + MD5

/**
 * Vindex parameters
 */
export interface VindexParams {
  /** Hash function to use */
  hash_function?: 'md5' | 'xxhash' | 'murmur3';
  /** For range vindex: range boundaries */
  ranges?: RangeBoundary[];
  /** For lookup vindex: lookup table name */
  table?: string;
  /** For lookup vindex: from columns */
  from?: string[];
  /** For lookup vindex: to column */
  to?: string;
  /** For consistent hash: number of virtual nodes */
  vnodes?: number;
  /** For lookup vindex: unique flag */
  unique?: boolean;
}

/**
 * Range boundary for range vindex
 */
export interface RangeBoundary {
  /** Lower bound (inclusive) */
  from: bigint | number | string;
  /** Upper bound (exclusive) */
  to: bigint | number | string;
  /** Target shard */
  shard: string;
}

/**
 * Keyspace ID (shard key value)
 */
export type KeyspaceId = Uint8Array;

/**
 * Destination represents where to route a query
 */
export interface Destination {
  /** Target shard(s) */
  shards: string[];
  /** Whether the destination is unique (single shard) */
  unique: boolean;
}

/**
 * Vindex interface
 */
export interface Vindex {
  /** Vindex type */
  readonly type: VindexType;
  /** Whether this vindex is unique */
  readonly unique: boolean;
  /** Whether this vindex requires verification (lookup vindexes) */
  readonly needsVCursor: boolean;

  /**
   * Map a value to keyspace ID(s)
   */
  map(value: unknown): KeyspaceId[];

  /**
   * Verify that a value exists (for lookup vindexes)
   */
  verify?(values: unknown[], keyspaceIds: KeyspaceId[]): Promise<boolean[]>;

  /**
   * Create an entry (for lookup vindexes)
   */
  create?(values: unknown[], keyspaceIds: KeyspaceId[]): Promise<void>;

  /**
   * Delete an entry (for lookup vindexes)
   */
  delete?(values: unknown[], keyspaceIds: KeyspaceId[]): Promise<void>;
}

/**
 * Simple MD5-like hash implementation (simplified for browser/edge compatibility)
 */
function simpleHash(str: string): Uint8Array {
  let h1 = 0xdeadbeef;
  let h2 = 0x41c6ce57;

  for (let i = 0; i < str.length; i++) {
    const ch = str.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }

  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507);
  h1 ^= Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507);
  h2 ^= Math.imul(h1 ^ (h1 >>> 13), 3266489909);

  const result = new Uint8Array(8);
  const view = new DataView(result.buffer);
  view.setUint32(0, h1 >>> 0, false);
  view.setUint32(4, h2 >>> 0, false);
  return result;
}

/**
 * xxHash-style hash (simplified)
 */
function xxhashStyle(str: string): Uint8Array {
  const PRIME1 = 2654435761;
  const PRIME2 = 2246822519;
  const PRIME3 = 3266489917;
  const PRIME4 = 668265263;
  const PRIME5 = 374761393;

  let h32 = PRIME5;
  const len = str.length;

  for (let i = 0; i < len; i++) {
    h32 += str.charCodeAt(i) * PRIME3;
    h32 = Math.imul((h32 << 17) | (h32 >>> 15), PRIME4);
  }

  h32 ^= len;
  h32 ^= h32 >>> 15;
  h32 = Math.imul(h32, PRIME2);
  h32 ^= h32 >>> 13;
  h32 = Math.imul(h32, PRIME3);
  h32 ^= h32 >>> 16;

  // Generate 64-bit result
  let h64_high = h32;
  let h64_low = Math.imul(h32, PRIME1) ^ PRIME2;

  const result = new Uint8Array(8);
  const view = new DataView(result.buffer);
  view.setUint32(0, h64_high >>> 0, false);
  view.setUint32(4, h64_low >>> 0, false);
  return result;
}

/**
 * murmur3-style hash (simplified)
 */
function murmur3Style(str: string): Uint8Array {
  const c1 = 0xcc9e2d51;
  const c2 = 0x1b873593;
  let h1 = 0;

  for (let i = 0; i < str.length; i++) {
    let k1 = str.charCodeAt(i);
    k1 = Math.imul(k1, c1);
    k1 = (k1 << 15) | (k1 >>> 17);
    k1 = Math.imul(k1, c2);
    h1 ^= k1;
    h1 = (h1 << 13) | (h1 >>> 19);
    h1 = Math.imul(h1, 5) + 0xe6546b64;
  }

  h1 ^= str.length;
  h1 ^= h1 >>> 16;
  h1 = Math.imul(h1, 0x85ebca6b);
  h1 ^= h1 >>> 13;
  h1 = Math.imul(h1, 0xc2b2ae35);
  h1 ^= h1 >>> 16;

  // Generate second 32 bits differently
  let h2 = Math.imul(h1, 0x9e3779b9);

  const result = new Uint8Array(8);
  const view = new DataView(result.buffer);
  view.setUint32(0, h1 >>> 0, false);
  view.setUint32(4, h2 >>> 0, false);
  return result;
}

/**
 * Convert value to string for hashing
 */
function valueToString(value: unknown): string {
  if (value === null || value === undefined) {
    throw new Error('Cannot hash null or undefined value');
  }
  if (typeof value === 'bigint') {
    return value.toString();
  }
  if (typeof value === 'number') {
    return value.toString();
  }
  if (typeof value === 'string') {
    return value;
  }
  if (value instanceof Uint8Array) {
    return Array.from(value).map(b => String.fromCharCode(b)).join('');
  }
  return String(value);
}

/**
 * Hash vindex - MD5/murmur hash of column value
 */
export class HashVindex implements Vindex {
  readonly type: VindexType = 'hash';
  readonly unique = true;
  readonly needsVCursor = false;

  private hashFunction: 'md5' | 'xxhash' | 'murmur3';

  constructor(params?: VindexParams) {
    this.hashFunction = params?.hash_function ?? 'md5';
  }

  map(value: unknown): KeyspaceId[] {
    if (value === null || value === undefined) {
      throw new Error('Cannot hash null or undefined value');
    }

    const str = valueToString(value);

    let result: Uint8Array;
    switch (this.hashFunction) {
      case 'xxhash':
        result = xxhashStyle(str);
        break;
      case 'murmur3':
        result = murmur3Style(str);
        break;
      case 'md5':
      default:
        result = simpleHash(str);
        break;
    }

    return [result];
  }
}

/**
 * Consistent hash vindex - ketama-style consistent hashing
 */
export class ConsistentHashVindex implements Vindex {
  readonly type: VindexType = 'consistent_hash';
  readonly unique = true;
  readonly needsVCursor = false;

  private vnodes: number;
  private ring: Map<number, string> = new Map();
  private sortedKeys: number[] = [];

  constructor(params?: VindexParams) {
    this.vnodes = params?.vnodes ?? 150;
  }

  /**
   * Initialize the hash ring with shards
   */
  initRing(shards: string[]): void {
    this.ring.clear();
    this.sortedKeys = [];

    for (const shard of shards) {
      for (let i = 0; i < this.vnodes; i++) {
        const key = `${shard}:${i}`;
        const hash = this.hashKey(key);
        this.ring.set(hash, shard);
        this.sortedKeys.push(hash);
      }
    }

    this.sortedKeys.sort((a, b) => a - b);
  }

  private hashKey(key: string): number {
    let hash = 0;
    for (let i = 0; i < key.length; i++) {
      const char = key.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return hash >>> 0; // Make unsigned
  }

  map(value: unknown): KeyspaceId[] {
    if (value === null || value === undefined) {
      throw new Error('Cannot hash null or undefined value');
    }

    const str = valueToString(value);
    const hash = simpleHash(str);

    return [hash];
  }

  /**
   * Get target shard for a keyspace ID
   */
  getShard(keyspaceId: KeyspaceId): string {
    if (this.sortedKeys.length === 0) {
      throw new Error('Ring not initialized - call initRing first');
    }

    // Convert keyspace ID to number for ring lookup
    const view = new DataView(keyspaceId.buffer, keyspaceId.byteOffset, keyspaceId.byteLength);
    const hash = view.getUint32(0, false);

    // Binary search for the first key >= hash
    let low = 0;
    let high = this.sortedKeys.length;

    while (low < high) {
      const mid = (low + high) >>> 1;
      if (this.sortedKeys[mid] < hash) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }

    // Wrap around if needed
    const index = low >= this.sortedKeys.length ? 0 : low;
    const ringKey = this.sortedKeys[index];
    return this.ring.get(ringKey)!;
  }
}

/**
 * Range vindex - numeric range-based sharding
 */
export class RangeVindex implements Vindex {
  readonly type: VindexType = 'range';
  readonly unique = true;
  readonly needsVCursor = false;

  private ranges: RangeBoundary[];

  constructor(params?: VindexParams) {
    this.ranges = params?.ranges ?? [];
  }

  map(value: unknown): KeyspaceId[] {
    if (value === null || value === undefined) {
      throw new Error('Cannot map null or undefined value');
    }

    const numValue = typeof value === 'bigint' ? value : BigInt(Number(value));
    const shard = this.findShard(numValue);

    // Generate a keyspace ID based on the value
    const result = new Uint8Array(8);
    const view = new DataView(result.buffer);

    // Use the numeric value to create the keyspace ID
    if (numValue <= BigInt(Number.MAX_SAFE_INTEGER)) {
      const num = Number(numValue);
      view.setUint32(0, (num / 0x100000000) >>> 0, false);
      view.setUint32(4, num >>> 0, false);
    } else {
      // For bigint, take lower 64 bits
      const low = Number(numValue & BigInt(0xFFFFFFFF));
      const high = Number((numValue >> BigInt(32)) & BigInt(0xFFFFFFFF));
      view.setUint32(0, high >>> 0, false);
      view.setUint32(4, low >>> 0, false);
    }

    return [result];
  }

  /**
   * Find shard for a numeric value
   */
  findShard(value: bigint | number): string | undefined {
    const numValue = typeof value === 'bigint' ? value : BigInt(value);

    for (const range of this.ranges) {
      const from = typeof range.from === 'bigint' ? range.from :
                   typeof range.from === 'string' ? BigInt(range.from) : BigInt(range.from);
      const to = typeof range.to === 'bigint' ? range.to :
                 typeof range.to === 'string' ? BigInt(range.to) : BigInt(range.to);

      if (numValue >= from && numValue < to) {
        return range.shard;
      }
    }

    return undefined;
  }
}

/**
 * Lookup vindex - requires secondary table lookup
 */
export class LookupVindex implements Vindex {
  readonly type: VindexType = 'lookup';
  readonly unique: boolean;
  readonly needsVCursor = true;

  private tableName: string;
  private fromColumns: string[];
  private toColumn: string;

  // In-memory storage for test purposes
  private storage: Map<string, Uint8Array[]> = new Map();

  constructor(params: VindexParams & { unique?: boolean }) {
    this.tableName = params.table ?? '';
    this.fromColumns = params.from ?? [];
    this.toColumn = params.to ?? '';
    this.unique = params.unique ?? false;
  }

  map(value: unknown): KeyspaceId[] {
    // Lookup vindexes cannot map synchronously
    throw new Error('Lookup vindex requires async verification');
  }

  async verify(values: unknown[], keyspaceIds: KeyspaceId[]): Promise<boolean[]> {
    const results: boolean[] = [];

    for (let i = 0; i < values.length; i++) {
      const key = String(values[i]);
      const stored = this.storage.get(key);

      if (!stored) {
        results.push(false);
      } else {
        // Check if the keyspace ID matches any stored
        const matches = stored.some(storedId =>
          keyspaceIds[i].length === storedId.length &&
          keyspaceIds[i].every((b, j) => b === storedId[j])
        );
        results.push(matches);
      }
    }

    return results;
  }

  async create(values: unknown[], keyspaceIds: KeyspaceId[]): Promise<void> {
    for (let i = 0; i < values.length; i++) {
      const key = String(values[i]);
      const existing = this.storage.get(key) ?? [];
      existing.push(keyspaceIds[i]);
      this.storage.set(key, existing);
    }
  }

  async delete(values: unknown[], keyspaceIds: KeyspaceId[]): Promise<void> {
    for (let i = 0; i < values.length; i++) {
      const key = String(values[i]);
      const existing = this.storage.get(key);

      if (existing) {
        const filtered = existing.filter(storedId =>
          keyspaceIds[i].length !== storedId.length ||
          !keyspaceIds[i].every((b, j) => b === storedId[j])
        );
        if (filtered.length === 0) {
          this.storage.delete(key);
        } else {
          this.storage.set(key, filtered);
        }
      }
    }
  }
}

/**
 * Create a vindex by type
 */
export function createVindex(type: VindexType, params?: VindexParams): Vindex {
  switch (type) {
    case 'hash':
    case 'binary_md5':
    case 'unicode_loose_md5':
      return new HashVindex(params);
    case 'consistent_hash':
      return new ConsistentHashVindex(params);
    case 'range':
    case 'numeric':
      return new RangeVindex(params);
    case 'lookup':
    case 'lookup_hash':
      return new LookupVindex(params as VindexParams & { unique?: boolean });
    case 'lookup_unique':
      return new LookupVindex({ ...params, unique: true } as VindexParams & { unique: boolean });
    default:
      throw new Error(`Unknown vindex type: ${type}`);
  }
}

/**
 * Compute keyspace ID from value using specified vindex
 */
export function computeKeyspaceId(vindex: Vindex, value: unknown): KeyspaceId {
  const ids = vindex.map(value);
  if (ids.length === 0) {
    throw new Error('Vindex returned no keyspace IDs');
  }
  return ids[0];
}

/**
 * Route a keyspace ID to a shard
 */
export function routeToShard(keyspaceId: KeyspaceId, shards: string[]): string {
  if (shards.length === 0) {
    throw new Error('No shards available');
  }

  if (shards.length === 1) {
    return shards[0];
  }

  // Convert keyspace ID to bigint for comparison
  const view = new DataView(keyspaceId.buffer, keyspaceId.byteOffset, keyspaceId.byteLength);
  const high = view.getUint32(0, false);
  const low = view.getUint32(4, false);
  const keyspaceValue = (BigInt(high) << BigInt(32)) | BigInt(low);

  // Find the shard that contains this keyspace ID
  for (const shard of shards) {
    const range = parseShardRange(shard);
    if (keyspaceValue >= range.start && keyspaceValue < range.end) {
      return shard;
    }
  }

  // Should not reach here if shards cover full keyspace
  throw new Error(`No shard found for keyspace ID`);
}

/**
 * Parse shard range (e.g., '-80', '80-', '40-80')
 */
export function parseShardRange(shard: string): { start: bigint; end: bigint } {
  if (!shard) {
    throw new Error('Empty shard range');
  }

  // Unsharded range
  if (shard === '-') {
    return {
      start: BigInt(0),
      end: BigInt('0xffffffffffffffff') + BigInt(1),
    };
  }

  const parts = shard.split('-');
  if (parts.length !== 2) {
    throw new Error(`Invalid shard range format: ${shard}`);
  }

  const [startStr, endStr] = parts;

  // Parse start (empty means 0)
  const start = startStr
    ? BigInt('0x' + startStr.padEnd(16, '0'))
    : BigInt(0);

  // Parse end (empty means max+1)
  const end = endStr
    ? BigInt('0x' + endStr.padEnd(16, '0'))
    : BigInt('0xffffffffffffffff') + BigInt(1);

  if (start >= end) {
    throw new Error(`Invalid shard range: start (${start}) must be less than end (${end})`);
  }

  return { start, end };
}

/**
 * Check if a keyspace ID falls within a shard range
 */
export function keyspaceIdInShard(keyspaceId: KeyspaceId, shard: string): boolean {
  const range = parseShardRange(shard);

  // Convert keyspace ID to bigint
  const view = new DataView(keyspaceId.buffer, keyspaceId.byteOffset, keyspaceId.byteLength);
  const high = view.getUint32(0, false);
  const low = view.getUint32(4, false);
  const keyspaceValue = (BigInt(high) << BigInt(32)) | BigInt(low);

  return keyspaceValue >= range.start && keyspaceValue < range.end;
}
