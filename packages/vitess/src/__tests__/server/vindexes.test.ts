/**
 * Vindexes Tests - Hash, Consistent Hash, Range, Lookup Vindexes
 *
 * TDD Red tests for vindex implementations.
 * Vindexes determine which shard a row belongs to based on column values.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  HashVindex,
  ConsistentHashVindex,
  RangeVindex,
  LookupVindex,
  createVindex,
  computeKeyspaceId,
  routeToShard,
  parseShardRange,
  keyspaceIdInShard,
  type VindexType,
  type VindexParams,
  type KeyspaceId,
  type RangeBoundary,
} from '../../server/vindexes.js';

describe('HashVindex', () => {
  let vindex: HashVindex;

  beforeEach(() => {
    vindex = new HashVindex();
  });

  describe('map()', () => {
    it('should map integer to keyspace ID', () => {
      const keyspaceIds = vindex.map(123);

      expect(keyspaceIds).toHaveLength(1);
      expect(keyspaceIds[0]).toBeInstanceOf(Uint8Array);
      expect(keyspaceIds[0].length).toBe(8); // 64-bit keyspace ID
    });

    it('should map string to keyspace ID', () => {
      const keyspaceIds = vindex.map('user-123');

      expect(keyspaceIds).toHaveLength(1);
      expect(keyspaceIds[0]).toBeInstanceOf(Uint8Array);
    });

    it('should map bigint to keyspace ID', () => {
      const keyspaceIds = vindex.map(BigInt('9223372036854775807'));

      expect(keyspaceIds).toHaveLength(1);
    });

    it('should map UUID string to keyspace ID', () => {
      const keyspaceIds = vindex.map('550e8400-e29b-41d4-a716-446655440000');

      expect(keyspaceIds).toHaveLength(1);
    });

    it('should produce consistent results for same value', () => {
      const id1 = vindex.map(123);
      const id2 = vindex.map(123);

      expect(id1[0]).toEqual(id2[0]);
    });

    it('should produce different results for different values', () => {
      const id1 = vindex.map(123);
      const id2 = vindex.map(456);

      expect(id1[0]).not.toEqual(id2[0]);
    });

    it('should distribute values evenly across keyspace', () => {
      // Hash 1000 values and check distribution
      const buckets = new Map<number, number>();

      for (let i = 0; i < 1000; i++) {
        const keyspaceId = vindex.map(i)[0];
        // Use first byte as bucket
        const bucket = keyspaceId[0];
        buckets.set(bucket, (buckets.get(bucket) || 0) + 1);
      }

      // Check that values are spread across multiple buckets
      expect(buckets.size).toBeGreaterThan(100);
    });

    it('should handle null by throwing error', () => {
      expect(() => vindex.map(null)).toThrow();
    });

    it('should handle undefined by throwing error', () => {
      expect(() => vindex.map(undefined)).toThrow();
    });
  });

  describe('with custom hash function', () => {
    it('should use xxhash when configured', () => {
      const xxhashVindex = new HashVindex({ hash_function: 'xxhash' });

      const keyspaceId = xxhashVindex.map(123);

      expect(keyspaceId).toHaveLength(1);
    });

    it('should use murmur3 when configured', () => {
      const murmurVindex = new HashVindex({ hash_function: 'murmur3' });

      const keyspaceId = murmurVindex.map(123);

      expect(keyspaceId).toHaveLength(1);
    });

    it('should produce different results with different hash functions', () => {
      const md5Vindex = new HashVindex({ hash_function: 'md5' });
      const xxhashVindex = new HashVindex({ hash_function: 'xxhash' });

      const md5Id = md5Vindex.map(123);
      const xxhashId = xxhashVindex.map(123);

      expect(md5Id[0]).not.toEqual(xxhashId[0]);
    });
  });

  describe('properties', () => {
    it('should be unique', () => {
      expect(vindex.unique).toBe(true);
    });

    it('should not need VCursor', () => {
      expect(vindex.needsVCursor).toBe(false);
    });

    it('should have type hash', () => {
      expect(vindex.type).toBe('hash');
    });
  });
});

describe('ConsistentHashVindex', () => {
  let vindex: ConsistentHashVindex;

  beforeEach(() => {
    vindex = new ConsistentHashVindex({ vnodes: 150 });
    vindex.initRing(['-40', '40-80', '80-c0', 'c0-']);
  });

  describe('map()', () => {
    it('should map value to keyspace ID', () => {
      const keyspaceIds = vindex.map(123);

      expect(keyspaceIds).toHaveLength(1);
      expect(keyspaceIds[0]).toBeInstanceOf(Uint8Array);
    });

    it('should produce consistent results', () => {
      const id1 = vindex.map('user-123');
      const id2 = vindex.map('user-123');

      expect(id1[0]).toEqual(id2[0]);
    });
  });

  describe('getShard()', () => {
    it('should return shard for keyspace ID', () => {
      const keyspaceId = vindex.map(123)[0];
      const shard = vindex.getShard(keyspaceId);

      expect(['-40', '40-80', '80-c0', 'c0-']).toContain(shard);
    });

    it('should route same keyspace ID to same shard', () => {
      const keyspaceId = vindex.map(456)[0];
      const shard1 = vindex.getShard(keyspaceId);
      const shard2 = vindex.getShard(keyspaceId);

      expect(shard1).toBe(shard2);
    });

    it('should distribute load across shards', () => {
      const shardCounts = new Map<string, number>();

      for (let i = 0; i < 1000; i++) {
        const keyspaceId = vindex.map(i)[0];
        const shard = vindex.getShard(keyspaceId);
        shardCounts.set(shard, (shardCounts.get(shard) || 0) + 1);
      }

      // All shards should receive some traffic
      expect(shardCounts.size).toBe(4);

      // Check distribution is reasonably balanced (within 50% of expected)
      const expected = 1000 / 4;
      for (const count of shardCounts.values()) {
        expect(count).toBeGreaterThan(expected * 0.5);
        expect(count).toBeLessThan(expected * 1.5);
      }
    });
  });

  describe('initRing()', () => {
    it('should initialize with provided shards', () => {
      const newVindex = new ConsistentHashVindex();
      newVindex.initRing(['-80', '80-']);

      const keyspaceId = newVindex.map(123)[0];
      const shard = newVindex.getShard(keyspaceId);

      expect(['-80', '80-']).toContain(shard);
    });

    it('should use vnodes parameter', () => {
      const vindex100 = new ConsistentHashVindex({ vnodes: 100 });
      vindex100.initRing(['-80', '80-']);

      const vindex200 = new ConsistentHashVindex({ vnodes: 200 });
      vindex200.initRing(['-80', '80-']);

      // Both should work but may have different distributions
      const shard100 = vindex100.getShard(vindex100.map(123)[0]);
      const shard200 = vindex200.getShard(vindex200.map(123)[0]);

      expect(['-80', '80-']).toContain(shard100);
      expect(['-80', '80-']).toContain(shard200);
    });
  });

  describe('properties', () => {
    it('should be unique', () => {
      expect(vindex.unique).toBe(true);
    });

    it('should not need VCursor', () => {
      expect(vindex.needsVCursor).toBe(false);
    });
  });
});

describe('RangeVindex', () => {
  let vindex: RangeVindex;

  beforeEach(() => {
    vindex = new RangeVindex({
      ranges: [
        { from: 0, to: 1000, shard: '-40' },
        { from: 1000, to: 2000, shard: '40-80' },
        { from: 2000, to: 3000, shard: '80-c0' },
        { from: 3000, to: Number.MAX_SAFE_INTEGER, shard: 'c0-' },
      ],
    });
  });

  describe('map()', () => {
    it('should map value to keyspace ID', () => {
      const keyspaceIds = vindex.map(500);

      expect(keyspaceIds).toHaveLength(1);
      expect(keyspaceIds[0]).toBeInstanceOf(Uint8Array);
    });

    it('should map to correct range', () => {
      const id500 = vindex.map(500)[0];
      const id1500 = vindex.map(1500)[0];

      // Different ranges should produce different keyspace IDs
      expect(id500).not.toEqual(id1500);
    });
  });

  describe('findShard()', () => {
    it('should find correct shard for values in first range', () => {
      expect(vindex.findShard(0)).toBe('-40');
      expect(vindex.findShard(500)).toBe('-40');
      expect(vindex.findShard(999)).toBe('-40');
    });

    it('should find correct shard for values in middle ranges', () => {
      expect(vindex.findShard(1000)).toBe('40-80');
      expect(vindex.findShard(1500)).toBe('40-80');
      expect(vindex.findShard(2500)).toBe('80-c0');
    });

    it('should find correct shard for values in last range', () => {
      expect(vindex.findShard(3000)).toBe('c0-');
      expect(vindex.findShard(1000000)).toBe('c0-');
    });

    it('should handle boundary values', () => {
      // Boundary should go to next range (exclusive upper bound)
      expect(vindex.findShard(1000)).toBe('40-80');
      expect(vindex.findShard(2000)).toBe('80-c0');
      expect(vindex.findShard(3000)).toBe('c0-');
    });

    it('should return undefined for negative values if no range defined', () => {
      const noNegativeVindex = new RangeVindex({
        ranges: [{ from: 0, to: 1000, shard: '-' }],
      });

      expect(noNegativeVindex.findShard(-1)).toBeUndefined();
    });

    it('should handle bigint values', () => {
      const bigintVindex = new RangeVindex({
        ranges: [
          { from: BigInt(0), to: BigInt('9223372036854775807'), shard: '-80' },
          { from: BigInt('9223372036854775807'), to: BigInt('18446744073709551615'), shard: '80-' },
        ],
      });

      expect(bigintVindex.findShard(BigInt('1000000000000000000'))).toBe('-80');
    });
  });

  describe('properties', () => {
    it('should be unique', () => {
      expect(vindex.unique).toBe(true);
    });

    it('should not need VCursor', () => {
      expect(vindex.needsVCursor).toBe(false);
    });

    it('should have type range', () => {
      expect(vindex.type).toBe('range');
    });
  });
});

describe('LookupVindex', () => {
  let vindex: LookupVindex;

  beforeEach(() => {
    vindex = new LookupVindex({
      table: 'email_to_user',
      from: ['email'],
      to: 'user_id',
      unique: true,
    });
  });

  describe('map()', () => {
    it('should throw because lookup requires async verification', () => {
      expect(() => vindex.map('alice@example.com')).toThrow(/async|verification/i);
    });
  });

  describe('verify()', () => {
    it('should verify values exist in lookup table', async () => {
      const values = ['alice@example.com', 'bob@example.com'];
      const keyspaceIds = [new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8]), new Uint8Array([8, 7, 6, 5, 4, 3, 2, 1])];

      const results = await vindex.verify(values, keyspaceIds);

      expect(results).toHaveLength(2);
      expect(results.every((r) => typeof r === 'boolean')).toBe(true);
    });

    it('should return false for non-existent values', async () => {
      const values = ['nonexistent@example.com'];
      const keyspaceIds = [new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])];

      const results = await vindex.verify(values, keyspaceIds);

      expect(results[0]).toBe(false);
    });
  });

  describe('create()', () => {
    it('should create entries in lookup table', async () => {
      const values = ['new@example.com'];
      const keyspaceIds = [new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])];

      await expect(vindex.create(values, keyspaceIds)).resolves.toBeUndefined();
    });
  });

  describe('delete()', () => {
    it('should delete entries from lookup table', async () => {
      const values = ['delete@example.com'];
      const keyspaceIds = [new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8])];

      await expect(vindex.delete(values, keyspaceIds)).resolves.toBeUndefined();
    });
  });

  describe('properties', () => {
    it('should be unique when configured', () => {
      const uniqueLookup = new LookupVindex({ table: 't', from: ['c'], to: 'id', unique: true });
      expect(uniqueLookup.unique).toBe(true);
    });

    it('should not be unique by default', () => {
      const nonUniqueLookup = new LookupVindex({ table: 't', from: ['c'], to: 'id' });
      expect(nonUniqueLookup.unique).toBe(false);
    });

    it('should need VCursor', () => {
      expect(vindex.needsVCursor).toBe(true);
    });
  });
});

describe('createVindex()', () => {
  it('should create HashVindex for hash type', () => {
    const vindex = createVindex('hash');
    expect(vindex).toBeInstanceOf(HashVindex);
  });

  it('should create HashVindex for binary_md5 type', () => {
    const vindex = createVindex('binary_md5');
    expect(vindex).toBeInstanceOf(HashVindex);
  });

  it('should create ConsistentHashVindex for consistent_hash type', () => {
    const vindex = createVindex('consistent_hash');
    expect(vindex).toBeInstanceOf(ConsistentHashVindex);
  });

  it('should create RangeVindex for range type', () => {
    const vindex = createVindex('range');
    expect(vindex).toBeInstanceOf(RangeVindex);
  });

  it('should create RangeVindex for numeric type', () => {
    const vindex = createVindex('numeric');
    expect(vindex).toBeInstanceOf(RangeVindex);
  });

  it('should create LookupVindex for lookup type', () => {
    const vindex = createVindex('lookup', { table: 't', from: ['c'], to: 'id' });
    expect(vindex).toBeInstanceOf(LookupVindex);
    expect(vindex.unique).toBe(false);
  });

  it('should create unique LookupVindex for lookup_unique type', () => {
    const vindex = createVindex('lookup_unique', { table: 't', from: ['c'], to: 'id' });
    expect(vindex).toBeInstanceOf(LookupVindex);
    expect(vindex.unique).toBe(true);
  });

  it('should throw for unknown type', () => {
    expect(() => createVindex('unknown' as VindexType)).toThrow(/unknown/i);
  });

  it('should pass params to vindex constructor', () => {
    const vindex = createVindex('hash', { hash_function: 'xxhash' });
    expect(vindex.type).toBe('hash');
  });
});

describe('computeKeyspaceId()', () => {
  it('should compute keyspace ID using vindex', () => {
    const vindex = new HashVindex();
    const keyspaceId = computeKeyspaceId(vindex, 123);

    expect(keyspaceId).toBeInstanceOf(Uint8Array);
    expect(keyspaceId.length).toBe(8);
  });

  it('should throw if vindex returns no IDs', () => {
    const emptyVindex = {
      type: 'hash' as VindexType,
      unique: true,
      needsVCursor: false,
      map: () => [],
    };

    expect(() => computeKeyspaceId(emptyVindex, 123)).toThrow(/no keyspace/i);
  });
});

describe('routeToShard()', () => {
  it('should route keyspace ID to correct shard', () => {
    // Keyspace ID starting with 0x3F should go to first half
    const lowKeyspaceId = new Uint8Array([0x3f, 0, 0, 0, 0, 0, 0, 0]);
    const shard = routeToShard(lowKeyspaceId, ['-80', '80-']);

    expect(shard).toBe('-80');
  });

  it('should route high keyspace ID to second shard', () => {
    const highKeyspaceId = new Uint8Array([0x80, 0, 0, 0, 0, 0, 0, 0]);
    const shard = routeToShard(highKeyspaceId, ['-80', '80-']);

    expect(shard).toBe('80-');
  });

  it('should handle 4-way sharding', () => {
    const shards = ['-40', '40-80', '80-c0', 'c0-'];

    const id1 = new Uint8Array([0x20, 0, 0, 0, 0, 0, 0, 0]); // < 0x40
    const id2 = new Uint8Array([0x50, 0, 0, 0, 0, 0, 0, 0]); // 0x40-0x80
    const id3 = new Uint8Array([0xa0, 0, 0, 0, 0, 0, 0, 0]); // 0x80-0xc0
    const id4 = new Uint8Array([0xd0, 0, 0, 0, 0, 0, 0, 0]); // >= 0xc0

    expect(routeToShard(id1, shards)).toBe('-40');
    expect(routeToShard(id2, shards)).toBe('40-80');
    expect(routeToShard(id3, shards)).toBe('80-c0');
    expect(routeToShard(id4, shards)).toBe('c0-');
  });
});

describe('parseShardRange()', () => {
  it('should parse open-start range', () => {
    const range = parseShardRange('-80');

    expect(range.start).toBe(BigInt(0));
    expect(range.end).toBe(BigInt('0x8000000000000000'));
  });

  it('should parse open-end range', () => {
    const range = parseShardRange('80-');

    expect(range.start).toBe(BigInt('0x8000000000000000'));
    expect(range.end).toBe(BigInt('0xffffffffffffffff') + BigInt(1));
  });

  it('should parse closed range', () => {
    const range = parseShardRange('40-80');

    expect(range.start).toBe(BigInt('0x4000000000000000'));
    expect(range.end).toBe(BigInt('0x8000000000000000'));
  });

  it('should parse unsharded range', () => {
    const range = parseShardRange('-');

    expect(range.start).toBe(BigInt(0));
    expect(range.end).toBe(BigInt('0xffffffffffffffff') + BigInt(1));
  });

  it('should throw for invalid format', () => {
    expect(() => parseShardRange('invalid')).toThrow();
    expect(() => parseShardRange('')).toThrow();
    expect(() => parseShardRange('80-40')).toThrow(); // start > end
  });
});

describe('keyspaceIdInShard()', () => {
  it('should return true when keyspace ID is in shard', () => {
    const keyspaceId = new Uint8Array([0x20, 0, 0, 0, 0, 0, 0, 0]);

    expect(keyspaceIdInShard(keyspaceId, '-40')).toBe(true);
    expect(keyspaceIdInShard(keyspaceId, '-80')).toBe(true);
    expect(keyspaceIdInShard(keyspaceId, '-')).toBe(true);
  });

  it('should return false when keyspace ID is not in shard', () => {
    const keyspaceId = new Uint8Array([0x80, 0, 0, 0, 0, 0, 0, 0]);

    expect(keyspaceIdInShard(keyspaceId, '-40')).toBe(false);
    expect(keyspaceIdInShard(keyspaceId, '-80')).toBe(false);
    expect(keyspaceIdInShard(keyspaceId, '40-80')).toBe(false);
  });

  it('should handle boundary cases', () => {
    const boundaryId = new Uint8Array([0x80, 0, 0, 0, 0, 0, 0, 0]);

    // 0x80 is the boundary between -80 and 80-
    expect(keyspaceIdInShard(boundaryId, '-80')).toBe(false);
    expect(keyspaceIdInShard(boundaryId, '80-')).toBe(true);
  });
});
