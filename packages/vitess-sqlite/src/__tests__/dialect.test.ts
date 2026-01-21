/**
 * SQL Dialect Translation Tests (Postgres -> SQLite)
 *
 * Tests for translating PostgreSQL-style SQL to SQLite-compatible SQL.
 * Issue: vitess-1bb.9
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { TursoAdapter, translatePostgresToSQLite } from '../index.js';

describe('SQL Dialect Translation (Postgres -> SQLite)', () => {
  let adapter: TursoAdapter;

  beforeEach(async () => {
    adapter = new TursoAdapter({ url: ':memory:' });
    await adapter.connect();
  });

  afterEach(async () => {
    await adapter.close();
  });

  describe('translatePostgresToSQLite function', () => {
    describe('data type translations', () => {
      it('should translate SERIAL to INTEGER PRIMARY KEY AUTOINCREMENT', () => {
        const pg = 'CREATE TABLE t (id SERIAL PRIMARY KEY)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('INTEGER PRIMARY KEY AUTOINCREMENT');
        expect(sqlite).not.toContain('SERIAL');
      });

      it('should translate BIGSERIAL to INTEGER PRIMARY KEY AUTOINCREMENT', () => {
        const pg = 'CREATE TABLE t (id BIGSERIAL PRIMARY KEY)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('INTEGER PRIMARY KEY AUTOINCREMENT');
      });

      it('should translate SMALLSERIAL to INTEGER PRIMARY KEY AUTOINCREMENT', () => {
        const pg = 'CREATE TABLE t (id SMALLSERIAL PRIMARY KEY)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('INTEGER PRIMARY KEY AUTOINCREMENT');
      });

      it('should translate VARCHAR(n) to TEXT', () => {
        const pg = 'CREATE TABLE t (name VARCHAR(255))';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
        expect(sqlite).not.toContain('VARCHAR');
      });

      it('should translate CHAR(n) to TEXT', () => {
        const pg = 'CREATE TABLE t (code CHAR(10))';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
        expect(sqlite).not.toContain('CHAR(');
      });

      it('should translate BOOLEAN to INTEGER', () => {
        const pg = 'CREATE TABLE t (active BOOLEAN)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('INTEGER');
        expect(sqlite).not.toContain('BOOLEAN');
      });

      it('should translate TIMESTAMP to TEXT', () => {
        const pg = 'CREATE TABLE t (created_at TIMESTAMP)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
        expect(sqlite).not.toContain('TIMESTAMP');
      });

      it('should translate TIMESTAMPTZ to TEXT', () => {
        const pg = 'CREATE TABLE t (created_at TIMESTAMPTZ)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
      });

      it('should translate TIMESTAMP WITH TIME ZONE to TEXT', () => {
        const pg = 'CREATE TABLE t (created_at TIMESTAMP WITH TIME ZONE)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
      });

      it('should translate DATE to TEXT', () => {
        const pg = 'CREATE TABLE t (birth_date DATE)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
      });

      it('should translate TIME to TEXT', () => {
        const pg = 'CREATE TABLE t (start_time TIME)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
      });

      it('should translate UUID to TEXT', () => {
        const pg = 'CREATE TABLE t (id UUID)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
        expect(sqlite).not.toContain('UUID');
      });

      it('should translate JSONB to TEXT', () => {
        const pg = 'CREATE TABLE t (data JSONB)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
        expect(sqlite).not.toContain('JSONB');
      });

      it('should translate JSON to TEXT', () => {
        const pg = 'CREATE TABLE t (data JSON)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('TEXT');
      });

      it('should translate BYTEA to BLOB', () => {
        const pg = 'CREATE TABLE t (data BYTEA)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('BLOB');
        expect(sqlite).not.toContain('BYTEA');
      });

      it('should translate DOUBLE PRECISION to REAL', () => {
        const pg = 'CREATE TABLE t (value DOUBLE PRECISION)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('REAL');
      });

      it('should translate NUMERIC(p,s) to REAL', () => {
        const pg = 'CREATE TABLE t (price NUMERIC(10,2))';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('REAL');
      });

      it('should translate DECIMAL(p,s) to REAL', () => {
        const pg = 'CREATE TABLE t (price DECIMAL(10,2))';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('REAL');
      });

      it('should translate BIGINT to INTEGER', () => {
        const pg = 'CREATE TABLE t (big_num BIGINT)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('INTEGER');
      });

      it('should translate SMALLINT to INTEGER', () => {
        const pg = 'CREATE TABLE t (small_num SMALLINT)';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('INTEGER');
      });
    });

    describe('boolean value translations', () => {
      it('should translate TRUE to 1', () => {
        const pg = "INSERT INTO t (active) VALUES (TRUE)";
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('1');
        expect(sqlite).not.toMatch(/\bTRUE\b/i);
      });

      it('should translate FALSE to 0', () => {
        const pg = "INSERT INTO t (active) VALUES (FALSE)";
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('0');
        expect(sqlite).not.toMatch(/\bFALSE\b/i);
      });

      it('should translate boolean in WHERE clause', () => {
        const pg = 'SELECT * FROM t WHERE active = TRUE';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('= 1');
      });
    });

    describe('function translations', () => {
      it('should translate NOW() to datetime("now")', () => {
        const pg = 'SELECT NOW()';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite.toLowerCase()).toContain('datetime');
        expect(sqlite.toLowerCase()).toContain('now');
      });

      it('should translate CURRENT_TIMESTAMP to datetime("now")', () => {
        const pg = "INSERT INTO t (created) VALUES (CURRENT_TIMESTAMP)";
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite.toLowerCase()).toMatch(/datetime.*now|current_timestamp/i);
      });

      it('should translate COALESCE (should remain same)', () => {
        const pg = 'SELECT COALESCE(name, "Unknown") FROM t';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('COALESCE');
      });

      it('should translate NULLIF (should remain same)', () => {
        const pg = 'SELECT NULLIF(value, 0) FROM t';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('NULLIF');
      });

      it('should translate string concatenation || (should remain same)', () => {
        const pg = "SELECT first_name || ' ' || last_name FROM t";
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('||');
      });

      it('should translate EXTRACT(EPOCH FROM ...) to strftime', () => {
        const pg = 'SELECT EXTRACT(EPOCH FROM created_at) FROM t';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite.toLowerCase()).toContain('strftime');
      });

      it('should translate gen_random_uuid() to lower(hex(randomblob(16)))', () => {
        const pg = 'INSERT INTO t (id) VALUES (gen_random_uuid())';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite.toLowerCase()).toContain('randomblob');
      });
    });

    describe('operator translations', () => {
      it('should translate ILIKE to LIKE (SQLite is case-insensitive by default)', () => {
        const pg = "SELECT * FROM t WHERE name ILIKE '%john%'";
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('LIKE');
        expect(sqlite).not.toContain('ILIKE');
      });

      it('should translate regex operators ~ to GLOB or LIKE', () => {
        const pg = "SELECT * FROM t WHERE name ~ '^J'";
        const sqlite = translatePostgresToSQLite(pg);
        // SQLite doesn't have regex, should convert to LIKE or GLOB
        expect(sqlite).toMatch(/LIKE|GLOB/i);
      });

      it('should translate ::type cast to CAST', () => {
        const pg = 'SELECT id::TEXT FROM t';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('CAST');
      });

      it('should translate array operators to alternatives', () => {
        const pg = "SELECT * FROM t WHERE id = ANY(ARRAY[1, 2, 3])";
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('IN');
      });
    });

    describe('constraint translations', () => {
      it('should remove IF NOT EXISTS for columns (SQLite limitation)', () => {
        // SQLite doesn't support IF NOT EXISTS for columns, only for tables/indexes
        const pg = 'ALTER TABLE t ADD COLUMN IF NOT EXISTS name TEXT';
        const sqlite = translatePostgresToSQLite(pg);
        // Should either remove IF NOT EXISTS or handle differently
        expect(sqlite).toContain('ADD COLUMN');
      });

      it('should translate ON CONFLICT to SQLite syntax', () => {
        const pg =
          "INSERT INTO t (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name";
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('ON CONFLICT');
        expect(sqlite).toContain('DO UPDATE SET');
      });

      it('should translate ON CONFLICT DO NOTHING', () => {
        const pg = "INSERT INTO t (id, name) VALUES (1, 'test') ON CONFLICT DO NOTHING";
        const sqlite = translatePostgresToSQLite(pg);
        // SQLite uses INSERT OR IGNORE or ON CONFLICT ... DO NOTHING
        expect(sqlite).toMatch(/ON CONFLICT.*DO NOTHING|INSERT OR IGNORE/i);
      });
    });

    describe('RETURNING clause', () => {
      it('should preserve RETURNING clause (SQLite 3.35+ supports it)', () => {
        const pg = "INSERT INTO t (name) VALUES ('test') RETURNING id";
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('RETURNING');
      });

      it('should preserve RETURNING * clause', () => {
        const pg = "INSERT INTO t (name) VALUES ('test') RETURNING *";
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('RETURNING *');
      });
    });

    describe('complex queries', () => {
      it('should handle CTE (WITH clause)', () => {
        const pg = `
          WITH active_users AS (
            SELECT * FROM users WHERE active = TRUE
          )
          SELECT * FROM active_users WHERE age > 18
        `;
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('WITH');
        expect(sqlite).toContain('= 1'); // TRUE -> 1
      });

      it('should handle LIMIT/OFFSET', () => {
        const pg = 'SELECT * FROM t LIMIT 10 OFFSET 5';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('LIMIT');
        expect(sqlite).toContain('OFFSET');
      });

      it('should translate FETCH FIRST to LIMIT', () => {
        const pg = 'SELECT * FROM t FETCH FIRST 10 ROWS ONLY';
        const sqlite = translatePostgresToSQLite(pg);
        expect(sqlite).toContain('LIMIT 10');
      });
    });
  });

  describe('adapter dialect translation integration', () => {
    it('should automatically translate Postgres queries when dialect mode is enabled', async () => {
      const pgAdapter = new TursoAdapter({
        url: ':memory:',
        dialect: 'postgres', // Enable Postgres dialect translation
      });
      await pgAdapter.connect();

      // Create table with Postgres syntax
      await pgAdapter.execute(`
        CREATE TABLE users (
          id SERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          active BOOLEAN DEFAULT TRUE
        )
      `);

      // Insert with Postgres TRUE
      await pgAdapter.execute(
        "INSERT INTO users (name, active) VALUES ('Alice', TRUE)"
      );

      // Query should work
      const result = await pgAdapter.query('SELECT * FROM users');
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].active).toBe(1); // TRUE -> 1

      await pgAdapter.close();
    });

    it('should pass through SQLite queries without translation when dialect is sqlite', async () => {
      // Default adapter should not translate
      await adapter.execute(`
        CREATE TABLE test (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT
        )
      `);

      await adapter.execute("INSERT INTO test (name) VALUES ('test')");
      const result = await adapter.query('SELECT * FROM test');
      expect(result.rows).toHaveLength(1);
    });
  });

  describe('edge cases', () => {
    it('should handle mixed case keywords', () => {
      const pg = 'CREATE TABLE t (id Serial PRIMARY KEY)';
      const sqlite = translatePostgresToSQLite(pg);
      expect(sqlite).toContain('INTEGER PRIMARY KEY AUTOINCREMENT');
    });

    it('should preserve string literals containing keywords', () => {
      const pg = "INSERT INTO t (msg) VALUES ('The SERIAL number is TRUE')";
      const sqlite = translatePostgresToSQLite(pg);
      expect(sqlite).toContain("'The SERIAL number is TRUE'");
    });

    it('should handle quoted identifiers', () => {
      const pg = 'CREATE TABLE "User" ("serialNumber" VARCHAR(50))';
      const sqlite = translatePostgresToSQLite(pg);
      expect(sqlite).toContain('"User"');
      expect(sqlite).toContain('"serialNumber"');
    });

    it('should handle comments', () => {
      const pg = `
        -- This is a comment
        SELECT * FROM t WHERE active = TRUE -- inline comment
      `;
      const sqlite = translatePostgresToSQLite(pg);
      expect(sqlite).toContain('= 1');
      expect(sqlite).toContain('--');
    });

    it('should handle multi-statement SQL', () => {
      const pg = `
        CREATE TABLE t (id SERIAL PRIMARY KEY);
        INSERT INTO t DEFAULT VALUES;
      `;
      const sqlite = translatePostgresToSQLite(pg);
      expect(sqlite).toContain('INTEGER PRIMARY KEY AUTOINCREMENT');
      expect(sqlite).toContain('INSERT INTO t DEFAULT VALUES');
    });
  });
});
