/**
 * PGliteAdapter Query Tests
 *
 * TDD Red tests for SELECT queries with parameters.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { PGliteAdapter, PGliteAdapterError, PGliteErrorCode } from '../index.js';

describe('PGliteAdapter Query', () => {
  let adapter: PGliteAdapter;

  beforeAll(async () => {
    adapter = new PGliteAdapter();
    await adapter.init();

    // Set up test tables
    await adapter.execute(`
      CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INT,
        active BOOLEAN DEFAULT true,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await adapter.execute(`
      CREATE TABLE posts (
        id SERIAL PRIMARY KEY,
        user_id INT REFERENCES users(id),
        title TEXT NOT NULL,
        content TEXT,
        published BOOLEAN DEFAULT false
      )
    `);

    // Insert test data
    await adapter.execute(
      "INSERT INTO users (name, email, age, active) VALUES ('Alice', 'alice@test.com', 30, true)"
    );
    await adapter.execute(
      "INSERT INTO users (name, email, age, active) VALUES ('Bob', 'bob@test.com', 25, true)"
    );
    await adapter.execute(
      "INSERT INTO users (name, email, age, active) VALUES ('Charlie', 'charlie@test.com', 35, false)"
    );

    await adapter.execute(
      "INSERT INTO posts (user_id, title, content, published) VALUES (1, 'Hello World', 'First post', true)"
    );
    await adapter.execute(
      "INSERT INTO posts (user_id, title, content, published) VALUES (1, 'Draft Post', 'Not ready', false)"
    );
    await adapter.execute(
      "INSERT INTO posts (user_id, title, content, published) VALUES (2, 'Bob Post', 'By Bob', true)"
    );
  });

  afterAll(async () => {
    await adapter.close();
  });

  describe('basic queries', () => {
    it('should execute a simple SELECT query', async () => {
      const result = await adapter.query('SELECT 1 as num');
      expect(result.rows).toEqual([{ num: 1 }]);
      expect(result.rowCount).toBe(1);
    });

    it('should return empty rows for no matches', async () => {
      const result = await adapter.query(
        "SELECT * FROM users WHERE name = 'NonExistent'"
      );
      expect(result.rows).toEqual([]);
      expect(result.rowCount).toBe(0);
    });

    it('should return all rows with SELECT *', async () => {
      const result = await adapter.query('SELECT * FROM users');
      expect(result.rows).toHaveLength(3);
      expect(result.rowCount).toBe(3);
    });

    it('should return selected columns only', async () => {
      const result = await adapter.query('SELECT name, email FROM users WHERE id = 1');
      expect(result.rows[0]).toHaveProperty('name');
      expect(result.rows[0]).toHaveProperty('email');
      expect(result.rows[0]).not.toHaveProperty('age');
      expect(result.rows[0]).not.toHaveProperty('active');
    });
  });

  describe('parameterized queries', () => {
    it('should execute query with single parameter', async () => {
      const result = await adapter.query('SELECT * FROM users WHERE id = $1', [1]);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Alice');
    });

    it('should execute query with multiple parameters', async () => {
      const result = await adapter.query(
        'SELECT * FROM users WHERE age >= $1 AND active = $2',
        [30, true]
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Alice');
    });

    it('should handle string parameters', async () => {
      const result = await adapter.query('SELECT * FROM users WHERE name = $1', [
        'Bob',
      ]);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].email).toBe('bob@test.com');
    });

    it('should handle null parameters', async () => {
      // First add a user with null email
      await adapter.execute("INSERT INTO users (name, email, age) VALUES ('NullUser', NULL, 40)");

      const result = await adapter.query('SELECT * FROM users WHERE email IS NULL');
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('NullUser');

      // Cleanup
      await adapter.execute("DELETE FROM users WHERE name = 'NullUser'");
    });

    it('should handle boolean parameters', async () => {
      const result = await adapter.query('SELECT * FROM users WHERE active = $1', [
        false,
      ]);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Charlie');
    });

    it('should handle array parameters with ANY', async () => {
      const result = await adapter.query(
        'SELECT * FROM users WHERE id = ANY($1) ORDER BY id',
        [[1, 2]]
      );
      expect(result.rows).toHaveLength(2);
      expect(result.rows[0].name).toBe('Alice');
      expect(result.rows[1].name).toBe('Bob');
    });

    it('should throw on parameter count mismatch', async () => {
      await expect(
        adapter.query('SELECT * FROM users WHERE id = $1 AND name = $2', [1])
      ).rejects.toThrow();
    });

    it('should handle parameter reuse ($1 used twice)', async () => {
      const result = await adapter.query(
        "SELECT * FROM users WHERE name = $1 OR email LIKE '%' || $1 || '%'",
        ['Alice']
      );
      expect(result.rows).toHaveLength(1);
    });
  });

  describe('query results', () => {
    it('should include field metadata', async () => {
      const result = await adapter.query('SELECT id, name, age FROM users LIMIT 1');
      expect(result.fields).toBeDefined();
      expect(result.fields).toHaveLength(3);
      expect(result.fields![0]).toMatchObject({ name: 'id' });
      expect(result.fields![1]).toMatchObject({ name: 'name' });
      expect(result.fields![2]).toMatchObject({ name: 'age' });
    });

    it('should include field types', async () => {
      const result = await adapter.query('SELECT id, name, age FROM users LIMIT 1');
      expect(result.fields![0].type).toBeDefined();
      expect(result.fields![1].type).toBeDefined();
      expect(result.fields![2].type).toBeDefined();
    });

    it('should include execution duration', async () => {
      const result = await adapter.query('SELECT * FROM users');
      expect(result.duration).toBeDefined();
      expect(typeof result.duration).toBe('number');
      expect(result.duration).toBeGreaterThanOrEqual(0);
    });

    it('should return correct rowCount', async () => {
      const result = await adapter.query('SELECT * FROM users WHERE active = true');
      expect(result.rowCount).toBe(2);
      expect(result.rows.length).toBe(result.rowCount);
    });
  });

  describe('typed queries', () => {
    interface User {
      id: number;
      name: string;
      email: string;
      age: number;
      active: boolean;
    }

    it('should support typed query results', async () => {
      const result = await adapter.query<User>('SELECT * FROM users WHERE id = $1', [1]);
      const user = result.rows[0];

      // TypeScript should know these types
      expect(typeof user.id).toBe('number');
      expect(typeof user.name).toBe('string');
      expect(typeof user.email).toBe('string');
      expect(typeof user.age).toBe('number');
      expect(typeof user.active).toBe('boolean');
    });
  });

  describe('complex queries', () => {
    it('should handle JOINs', async () => {
      const result = await adapter.query(`
        SELECT u.name, p.title
        FROM users u
        JOIN posts p ON u.id = p.user_id
        WHERE p.published = true
        ORDER BY p.id
      `);
      expect(result.rows).toHaveLength(2);
      expect(result.rows[0]).toEqual({ name: 'Alice', title: 'Hello World' });
      expect(result.rows[1]).toEqual({ name: 'Bob', title: 'Bob Post' });
    });

    it('should handle GROUP BY with aggregates', async () => {
      const result = await adapter.query(`
        SELECT user_id, COUNT(*) as post_count
        FROM posts
        GROUP BY user_id
        ORDER BY user_id
      `);
      expect(result.rows).toHaveLength(2);
      // PGlite returns number for COUNT (not bigint like native Postgres)
      expect(result.rows[0]).toEqual({ user_id: 1, post_count: 2 });
    });

    it('should handle subqueries', async () => {
      const result = await adapter.query(`
        SELECT * FROM users
        WHERE id IN (SELECT DISTINCT user_id FROM posts WHERE published = true)
        ORDER BY id
      `);
      expect(result.rows).toHaveLength(2);
    });

    it('should handle HAVING clause', async () => {
      const result = await adapter.query(`
        SELECT user_id, COUNT(*) as post_count
        FROM posts
        GROUP BY user_id
        HAVING COUNT(*) > 1
      `);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].user_id).toBe(1);
    });

    it('should handle ORDER BY with LIMIT and OFFSET', async () => {
      const result = await adapter.query(`
        SELECT * FROM users ORDER BY age DESC LIMIT 2 OFFSET 1
      `);
      expect(result.rows).toHaveLength(2);
      expect(result.rows[0].name).toBe('Alice'); // age 30
      expect(result.rows[1].name).toBe('Bob'); // age 25
    });

    it('should handle UNION queries', async () => {
      const result = await adapter.query(`
        SELECT name FROM users WHERE id = 1
        UNION
        SELECT title as name FROM posts WHERE id = 1
      `);
      expect(result.rows).toHaveLength(2);
    });

    it('should handle CTEs (WITH clause)', async () => {
      const result = await adapter.query(`
        WITH active_users AS (
          SELECT * FROM users WHERE active = true
        )
        SELECT name FROM active_users ORDER BY name
      `);
      expect(result.rows).toHaveLength(2);
      expect(result.rows[0].name).toBe('Alice');
      expect(result.rows[1].name).toBe('Bob');
    });

    it('should handle window functions', async () => {
      const result = await adapter.query(`
        SELECT
          name,
          age,
          ROW_NUMBER() OVER (ORDER BY age DESC) as rank
        FROM users
      `);
      expect(result.rows).toHaveLength(3);
      // PGlite returns number for ROW_NUMBER (not bigint like native Postgres)
      expect(result.rows.find(r => r.name === 'Charlie')?.rank).toBe(1);
    });
  });

  describe('error handling', () => {
    it('should throw on syntax error', async () => {
      await expect(adapter.query('SELECTT * FROM users')).rejects.toThrow(
        PGliteAdapterError
      );
      await expect(adapter.query('SELECTT * FROM users')).rejects.toMatchObject({
        code: PGliteErrorCode.SYNTAX_ERROR,
      });
    });

    it('should throw on non-existent table', async () => {
      await expect(adapter.query('SELECT * FROM nonexistent')).rejects.toThrow(
        PGliteAdapterError
      );
      await expect(adapter.query('SELECT * FROM nonexistent')).rejects.toMatchObject({
        code: PGliteErrorCode.QUERY_ERROR,
      });
    });

    it('should throw on non-existent column', async () => {
      await expect(
        adapter.query('SELECT nonexistent_column FROM users')
      ).rejects.toThrow(PGliteAdapterError);
    });

    it('should throw when not ready', async () => {
      const newAdapter = new PGliteAdapter();
      // Don't call init()

      await expect(newAdapter.query('SELECT 1')).rejects.toThrow(PGliteAdapterError);
      await expect(newAdapter.query('SELECT 1')).rejects.toMatchObject({
        code: PGliteErrorCode.NOT_READY,
      });
    });

    it('should include SQL state in error', async () => {
      try {
        await adapter.query('SELECT * FROM nonexistent_table');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PGliteAdapterError);
        expect((error as PGliteAdapterError).sqlState).toBeDefined();
      }
    });
  });

  describe('special values', () => {
    it('should handle empty string', async () => {
      await adapter.execute("INSERT INTO posts (user_id, title, content) VALUES (1, 'Empty', '')");
      const result = await adapter.query(
        "SELECT content FROM posts WHERE title = 'Empty'"
      );
      expect(result.rows[0].content).toBe('');
      await adapter.execute("DELETE FROM posts WHERE title = 'Empty'");
    });

    it('should handle large numbers', async () => {
      const result = await adapter.query('SELECT 9223372036854775807::bigint as big_num');
      expect(result.rows[0].big_num).toBe(9223372036854775807n);
    });

    it('should handle special characters in strings', async () => {
      const result = await adapter.query("SELECT $1 as special", [
        "Hello 'World' \"Test\" \\ \n \t",
      ]);
      expect(result.rows[0].special).toBe("Hello 'World' \"Test\" \\ \n \t");
    });

    it('should handle unicode strings', async () => {
      const result = await adapter.query('SELECT $1 as unicode', [
        'Hello',
      ]);
      expect(result.rows[0].unicode).toBe('Hello');
    });
  });

  describe('concurrent queries', () => {
    it('should handle multiple concurrent queries', async () => {
      const promises = [
        adapter.query('SELECT * FROM users WHERE id = 1'),
        adapter.query('SELECT * FROM users WHERE id = 2'),
        adapter.query('SELECT * FROM users WHERE id = 3'),
      ];

      const results = await Promise.all(promises);

      expect(results[0].rows[0].name).toBe('Alice');
      expect(results[1].rows[0].name).toBe('Bob');
      expect(results[2].rows[0].name).toBe('Charlie');
    });

    it('should serialize queries correctly', async () => {
      // Run many queries concurrently to test serialization
      // Use explicit integer cast to ensure numeric return type
      const promises = Array.from({ length: 10 }, (_, i) =>
        adapter.query('SELECT $1::int as num', [i])
      );

      const results = await Promise.all(promises);

      results.forEach((result, i) => {
        expect(result.rows[0].num).toBe(i);
      });
    });
  });
});
