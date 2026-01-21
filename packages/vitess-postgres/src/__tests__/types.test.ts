/**
 * PGliteAdapter Type Mapping Tests
 *
 * TDD Red tests for PostgreSQL type to JavaScript type mapping.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { PGliteAdapter } from '../index.js';

describe('PGliteAdapter Type Mapping', () => {
  let adapter: PGliteAdapter;

  beforeAll(async () => {
    adapter = new PGliteAdapter();
    await adapter.init();

    // Create a table with various PostgreSQL types
    await adapter.execute(`
      CREATE TABLE type_test (
        id SERIAL PRIMARY KEY,
        -- Numeric types
        int_col INT,
        bigint_col BIGINT,
        smallint_col SMALLINT,
        real_col REAL,
        double_col DOUBLE PRECISION,
        numeric_col NUMERIC(10, 2),
        -- String types
        text_col TEXT,
        varchar_col VARCHAR(100),
        char_col CHAR(10),
        -- Boolean
        bool_col BOOLEAN,
        -- Date/Time types
        timestamp_col TIMESTAMP,
        timestamptz_col TIMESTAMPTZ,
        date_col DATE,
        time_col TIME,
        timetz_col TIMETZ,
        interval_col INTERVAL,
        -- Binary
        bytea_col BYTEA,
        -- JSON types
        json_col JSON,
        jsonb_col JSONB,
        -- UUID
        uuid_col UUID,
        -- Arrays
        int_array_col INT[],
        text_array_col TEXT[],
        -- Network types
        inet_col INET,
        cidr_col CIDR,
        macaddr_col MACADDR
      )
    `);
  });

  afterAll(async () => {
    await adapter.close();
  });

  beforeEach(async () => {
    await adapter.execute('DELETE FROM type_test');
  });

  describe('numeric types', () => {
    it('should map INT to number', async () => {
      await adapter.execute('INSERT INTO type_test (int_col) VALUES ($1)', [42]);
      const result = await adapter.query('SELECT int_col FROM type_test');
      expect(result.rows[0].int_col).toBe(42);
      expect(typeof result.rows[0].int_col).toBe('number');
    });

    it('should map BIGINT to bigint', async () => {
      const bigValue = 9007199254740993n; // Larger than Number.MAX_SAFE_INTEGER
      await adapter.execute('INSERT INTO type_test (bigint_col) VALUES ($1)', [bigValue]);
      const result = await adapter.query('SELECT bigint_col FROM type_test');
      expect(result.rows[0].bigint_col).toBe(bigValue);
      expect(typeof result.rows[0].bigint_col).toBe('bigint');
    });

    it('should map SMALLINT to number', async () => {
      await adapter.execute('INSERT INTO type_test (smallint_col) VALUES ($1)', [32767]);
      const result = await adapter.query('SELECT smallint_col FROM type_test');
      expect(result.rows[0].smallint_col).toBe(32767);
      expect(typeof result.rows[0].smallint_col).toBe('number');
    });

    it('should map REAL to number', async () => {
      await adapter.execute('INSERT INTO type_test (real_col) VALUES ($1)', [3.14]);
      const result = await adapter.query('SELECT real_col FROM type_test');
      expect(result.rows[0].real_col).toBeCloseTo(3.14, 2);
      expect(typeof result.rows[0].real_col).toBe('number');
    });

    it('should map DOUBLE PRECISION to number', async () => {
      await adapter.execute('INSERT INTO type_test (double_col) VALUES ($1)', [
        3.141592653589793,
      ]);
      const result = await adapter.query('SELECT double_col FROM type_test');
      expect(result.rows[0].double_col).toBeCloseTo(3.141592653589793, 10);
      expect(typeof result.rows[0].double_col).toBe('number');
    });

    it('should map NUMERIC/DECIMAL to string (for precision)', async () => {
      await adapter.execute('INSERT INTO type_test (numeric_col) VALUES ($1)', ['12345.67']);
      const result = await adapter.query('SELECT numeric_col FROM type_test');
      expect(result.rows[0].numeric_col).toBe('12345.67');
      expect(typeof result.rows[0].numeric_col).toBe('string');
    });

    it('should handle NULL numeric values', async () => {
      await adapter.execute('INSERT INTO type_test (int_col) VALUES ($1)', [null]);
      const result = await adapter.query('SELECT int_col FROM type_test');
      expect(result.rows[0].int_col).toBeNull();
    });

    it('should handle special float values', async () => {
      await adapter.execute(
        "INSERT INTO type_test (double_col) VALUES ('NaN'::double precision)"
      );
      const result = await adapter.query('SELECT double_col FROM type_test');
      expect(Number.isNaN(result.rows[0].double_col)).toBe(true);
    });

    it('should handle Infinity', async () => {
      await adapter.execute(
        "INSERT INTO type_test (double_col) VALUES ('Infinity'::double precision)"
      );
      const result = await adapter.query('SELECT double_col FROM type_test');
      expect(result.rows[0].double_col).toBe(Infinity);
    });

    it('should handle -Infinity', async () => {
      await adapter.execute(
        "INSERT INTO type_test (double_col) VALUES ('-Infinity'::double precision)"
      );
      const result = await adapter.query('SELECT double_col FROM type_test');
      expect(result.rows[0].double_col).toBe(-Infinity);
    });
  });

  describe('string types', () => {
    it('should map TEXT to string', async () => {
      await adapter.execute('INSERT INTO type_test (text_col) VALUES ($1)', [
        'Hello World',
      ]);
      const result = await adapter.query('SELECT text_col FROM type_test');
      expect(result.rows[0].text_col).toBe('Hello World');
      expect(typeof result.rows[0].text_col).toBe('string');
    });

    it('should map VARCHAR to string', async () => {
      await adapter.execute('INSERT INTO type_test (varchar_col) VALUES ($1)', [
        'Variable Length',
      ]);
      const result = await adapter.query('SELECT varchar_col FROM type_test');
      expect(result.rows[0].varchar_col).toBe('Variable Length');
    });

    it('should map CHAR to string with padding', async () => {
      await adapter.execute('INSERT INTO type_test (char_col) VALUES ($1)', ['ABC']);
      const result = await adapter.query('SELECT char_col FROM type_test');
      // CHAR pads with spaces to the defined length
      expect(result.rows[0].char_col).toBe('ABC       ');
    });

    it('should handle empty string', async () => {
      await adapter.execute('INSERT INTO type_test (text_col) VALUES ($1)', ['']);
      const result = await adapter.query('SELECT text_col FROM type_test');
      expect(result.rows[0].text_col).toBe('');
    });

    it('should handle unicode strings', async () => {
      const unicodeStr = 'Hello \u4e16\u754c \ud83d\udc4b';
      await adapter.execute('INSERT INTO type_test (text_col) VALUES ($1)', [unicodeStr]);
      const result = await adapter.query('SELECT text_col FROM type_test');
      expect(result.rows[0].text_col).toBe(unicodeStr);
    });

    it('should handle NULL string values', async () => {
      await adapter.execute('INSERT INTO type_test (text_col) VALUES ($1)', [null]);
      const result = await adapter.query('SELECT text_col FROM type_test');
      expect(result.rows[0].text_col).toBeNull();
    });
  });

  describe('boolean type', () => {
    it('should map BOOLEAN true to true', async () => {
      await adapter.execute('INSERT INTO type_test (bool_col) VALUES ($1)', [true]);
      const result = await adapter.query('SELECT bool_col FROM type_test');
      expect(result.rows[0].bool_col).toBe(true);
      expect(typeof result.rows[0].bool_col).toBe('boolean');
    });

    it('should map BOOLEAN false to false', async () => {
      await adapter.execute('INSERT INTO type_test (bool_col) VALUES ($1)', [false]);
      const result = await adapter.query('SELECT bool_col FROM type_test');
      expect(result.rows[0].bool_col).toBe(false);
    });

    it('should handle NULL boolean values', async () => {
      await adapter.execute('INSERT INTO type_test (bool_col) VALUES ($1)', [null]);
      const result = await adapter.query('SELECT bool_col FROM type_test');
      expect(result.rows[0].bool_col).toBeNull();
    });
  });

  describe('date/time types', () => {
    it('should map TIMESTAMP to Date', async () => {
      const date = new Date('2024-01-15T10:30:00.000Z');
      await adapter.execute('INSERT INTO type_test (timestamp_col) VALUES ($1)', [date]);
      const result = await adapter.query('SELECT timestamp_col FROM type_test');
      expect(result.rows[0].timestamp_col).toBeInstanceOf(Date);
      expect(result.rows[0].timestamp_col.toISOString()).toBe(date.toISOString());
    });

    it('should map TIMESTAMPTZ to Date', async () => {
      const date = new Date('2024-01-15T10:30:00.000Z');
      await adapter.execute('INSERT INTO type_test (timestamptz_col) VALUES ($1)', [date]);
      const result = await adapter.query('SELECT timestamptz_col FROM type_test');
      expect(result.rows[0].timestamptz_col).toBeInstanceOf(Date);
    });

    it('should map DATE to Date or string', async () => {
      await adapter.execute("INSERT INTO type_test (date_col) VALUES ('2024-01-15')");
      const result = await adapter.query('SELECT date_col FROM type_test');
      // Could be Date or string depending on implementation
      const dateValue = result.rows[0].date_col;
      if (dateValue instanceof Date) {
        expect(dateValue.getFullYear()).toBe(2024);
        expect(dateValue.getMonth()).toBe(0); // January
        expect(dateValue.getDate()).toBe(15);
      } else {
        expect(dateValue).toContain('2024-01-15');
      }
    });

    it('should map TIME to string', async () => {
      await adapter.execute("INSERT INTO type_test (time_col) VALUES ('14:30:00')");
      const result = await adapter.query('SELECT time_col FROM type_test');
      expect(result.rows[0].time_col).toContain('14:30:00');
    });

    it('should map INTERVAL to string or object', async () => {
      await adapter.execute("INSERT INTO type_test (interval_col) VALUES ('1 day 2 hours')");
      const result = await adapter.query('SELECT interval_col FROM type_test');
      const interval = result.rows[0].interval_col;
      // Could be string or parsed object
      expect(interval).toBeDefined();
    });

    it('should handle NULL date/time values', async () => {
      await adapter.execute('INSERT INTO type_test (timestamp_col) VALUES ($1)', [null]);
      const result = await adapter.query('SELECT timestamp_col FROM type_test');
      expect(result.rows[0].timestamp_col).toBeNull();
    });
  });

  describe('binary type', () => {
    it('should map BYTEA to Buffer/Uint8Array', async () => {
      const buffer = Buffer.from([0x48, 0x65, 0x6c, 0x6c, 0x6f]); // "Hello"
      await adapter.execute('INSERT INTO type_test (bytea_col) VALUES ($1)', [buffer]);
      const result = await adapter.query('SELECT bytea_col FROM type_test');
      const bytea = result.rows[0].bytea_col;

      expect(bytea).toBeDefined();
      // Could be Buffer, Uint8Array, or hex string depending on implementation
      if (Buffer.isBuffer(bytea) || bytea instanceof Uint8Array) {
        expect(bytea[0]).toBe(0x48);
      }
    });

    it('should handle empty BYTEA', async () => {
      const buffer = Buffer.from([]);
      await adapter.execute('INSERT INTO type_test (bytea_col) VALUES ($1)', [buffer]);
      const result = await adapter.query('SELECT bytea_col FROM type_test');
      const bytea = result.rows[0].bytea_col;

      if (Buffer.isBuffer(bytea) || bytea instanceof Uint8Array) {
        expect(bytea.length).toBe(0);
      }
    });
  });

  describe('JSON types', () => {
    it('should map JSON to parsed object', async () => {
      const obj = { name: 'Test', value: 42 };
      await adapter.execute('INSERT INTO type_test (json_col) VALUES ($1)', [
        JSON.stringify(obj),
      ]);
      const result = await adapter.query('SELECT json_col FROM type_test');
      expect(result.rows[0].json_col).toEqual(obj);
    });

    it('should map JSONB to parsed object', async () => {
      const obj = { key: 'value', nested: { a: 1, b: 2 } };
      await adapter.execute('INSERT INTO type_test (jsonb_col) VALUES ($1)', [
        JSON.stringify(obj),
      ]);
      const result = await adapter.query('SELECT jsonb_col FROM type_test');
      expect(result.rows[0].jsonb_col).toEqual(obj);
    });

    it('should handle JSON array', async () => {
      const arr = [1, 2, 3, 'four', { five: 5 }];
      await adapter.execute('INSERT INTO type_test (json_col) VALUES ($1)', [
        JSON.stringify(arr),
      ]);
      const result = await adapter.query('SELECT json_col FROM type_test');
      expect(result.rows[0].json_col).toEqual(arr);
    });

    it('should handle JSON null', async () => {
      await adapter.execute("INSERT INTO type_test (json_col) VALUES ('null')");
      const result = await adapter.query('SELECT json_col FROM type_test');
      expect(result.rows[0].json_col).toBeNull();
    });

    it('should handle JSON primitive values', async () => {
      await adapter.execute("INSERT INTO type_test (json_col) VALUES ('42')");
      const result = await adapter.query('SELECT json_col FROM type_test');
      expect(result.rows[0].json_col).toBe(42);
    });

    it('should handle NULL JSON column', async () => {
      await adapter.execute('INSERT INTO type_test (json_col) VALUES ($1)', [null]);
      const result = await adapter.query('SELECT json_col FROM type_test');
      expect(result.rows[0].json_col).toBeNull();
    });
  });

  describe('UUID type', () => {
    it('should map UUID to string', async () => {
      const uuid = '550e8400-e29b-41d4-a716-446655440000';
      await adapter.execute('INSERT INTO type_test (uuid_col) VALUES ($1)', [uuid]);
      const result = await adapter.query('SELECT uuid_col FROM type_test');
      expect(result.rows[0].uuid_col).toBe(uuid);
      expect(typeof result.rows[0].uuid_col).toBe('string');
    });

    it('should generate UUID with gen_random_uuid()', async () => {
      await adapter.execute(
        'INSERT INTO type_test (uuid_col) VALUES (gen_random_uuid())'
      );
      const result = await adapter.query('SELECT uuid_col FROM type_test');
      const uuid = result.rows[0].uuid_col;
      expect(uuid).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
      );
    });
  });

  describe('array types', () => {
    it('should map INT[] to number array', async () => {
      await adapter.execute('INSERT INTO type_test (int_array_col) VALUES ($1)', [
        [1, 2, 3, 4, 5],
      ]);
      const result = await adapter.query('SELECT int_array_col FROM type_test');
      expect(result.rows[0].int_array_col).toEqual([1, 2, 3, 4, 5]);
      expect(Array.isArray(result.rows[0].int_array_col)).toBe(true);
    });

    it('should map TEXT[] to string array', async () => {
      await adapter.execute('INSERT INTO type_test (text_array_col) VALUES ($1)', [
        ['a', 'b', 'c'],
      ]);
      const result = await adapter.query('SELECT text_array_col FROM type_test');
      expect(result.rows[0].text_array_col).toEqual(['a', 'b', 'c']);
    });

    it('should handle empty array', async () => {
      await adapter.execute("INSERT INTO type_test (int_array_col) VALUES ('{}'::int[])");
      const result = await adapter.query('SELECT int_array_col FROM type_test');
      expect(result.rows[0].int_array_col).toEqual([]);
    });

    it('should handle NULL in array', async () => {
      await adapter.execute(
        "INSERT INTO type_test (int_array_col) VALUES ('{1, NULL, 3}'::int[])"
      );
      const result = await adapter.query('SELECT int_array_col FROM type_test');
      expect(result.rows[0].int_array_col).toEqual([1, null, 3]);
    });
  });

  describe('network types', () => {
    it('should map INET to string', async () => {
      await adapter.execute("INSERT INTO type_test (inet_col) VALUES ('192.168.1.1')");
      const result = await adapter.query('SELECT inet_col FROM type_test');
      expect(result.rows[0].inet_col).toBe('192.168.1.1');
    });

    it('should handle INET with CIDR notation', async () => {
      await adapter.execute("INSERT INTO type_test (inet_col) VALUES ('192.168.1.0/24')");
      const result = await adapter.query('SELECT inet_col FROM type_test');
      expect(result.rows[0].inet_col).toContain('192.168.1.0');
    });

    it('should map CIDR to string', async () => {
      await adapter.execute("INSERT INTO type_test (cidr_col) VALUES ('192.168.1.0/24')");
      const result = await adapter.query('SELECT cidr_col FROM type_test');
      expect(result.rows[0].cidr_col).toBe('192.168.1.0/24');
    });

    it('should map MACADDR to string', async () => {
      await adapter.execute(
        "INSERT INTO type_test (macaddr_col) VALUES ('08:00:2b:01:02:03')"
      );
      const result = await adapter.query('SELECT macaddr_col FROM type_test');
      expect(result.rows[0].macaddr_col).toBe('08:00:2b:01:02:03');
    });

    it('should handle IPv6 addresses', async () => {
      await adapter.execute(
        "INSERT INTO type_test (inet_col) VALUES ('2001:0db8:85a3:0000:0000:8a2e:0370:7334')"
      );
      const result = await adapter.query('SELECT inet_col FROM type_test');
      expect(result.rows[0].inet_col).toContain('2001');
    });
  });

  describe('field metadata', () => {
    it('should include type information in field metadata', async () => {
      await adapter.execute(
        'INSERT INTO type_test (int_col, text_col, bool_col) VALUES (1, $1, true)',
        ['test']
      );
      const result = await adapter.query(
        'SELECT int_col, text_col, bool_col FROM type_test'
      );

      expect(result.fields).toBeDefined();
      expect(result.fields).toHaveLength(3);

      // Field types should be mapped to readable names
      const intField = result.fields!.find((f) => f.name === 'int_col');
      const textField = result.fields!.find((f) => f.name === 'text_col');
      const boolField = result.fields!.find((f) => f.name === 'bool_col');

      expect(intField?.type).toMatch(/int/i);
      expect(textField?.type).toMatch(/text/i);
      expect(boolField?.type).toMatch(/bool/i);
    });

    it('should include native type ID in field metadata', async () => {
      const result = await adapter.query('SELECT 1::int as num');
      expect(result.fields![0].nativeType).toBeDefined();
      expect(typeof result.fields![0].nativeType).toBe('number');
    });
  });

  describe('type coercion edge cases', () => {
    it('should handle numeric string to number coercion', async () => {
      const result = await adapter.query("SELECT '123'::int as num");
      expect(result.rows[0].num).toBe(123);
    });

    it('should handle boolean string to boolean coercion', async () => {
      const result = await adapter.query("SELECT 'true'::boolean as flag");
      expect(result.rows[0].flag).toBe(true);
    });

    it('should handle timestamp string to Date coercion', async () => {
      const result = await adapter.query(
        "SELECT '2024-01-15 10:30:00'::timestamp as ts"
      );
      expect(result.rows[0].ts).toBeInstanceOf(Date);
    });

    it('should handle aggregate functions returning correct types', async () => {
      await adapter.execute('INSERT INTO type_test (int_col) VALUES (10), (20), (30)');

      const result = await adapter.query(`
        SELECT
          COUNT(*) as count_val,
          SUM(int_col) as sum_val,
          AVG(int_col) as avg_val,
          MIN(int_col) as min_val,
          MAX(int_col) as max_val
        FROM type_test
      `);

      // COUNT returns bigint
      expect(typeof result.rows[0].count_val).toBe('bigint');
      // SUM of INT returns bigint
      expect(typeof result.rows[0].sum_val).toBe('bigint');
      // AVG returns numeric (string for precision)
      expect(typeof result.rows[0].avg_val).toBe('string');
      // MIN/MAX return the column type
      expect(typeof result.rows[0].min_val).toBe('number');
      expect(typeof result.rows[0].max_val).toBe('number');
    });
  });
});
