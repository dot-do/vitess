/**
 * TursoAdapter Query (SELECT) Tests
 *
 * Tests for SELECT query execution against SQLite via Turso/libSQL.
 * Issue: vitess-1bb.3
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { TursoAdapter } from '../index.js';
import type { QueryResult, Row, Field } from '@dotdo/vitess-rpc';

describe('TursoAdapter Query Execution', () => {
  let adapter: TursoAdapter;

  beforeEach(async () => {
    adapter = new TursoAdapter({ url: ':memory:' });
    await adapter.connect();

    // Create test table
    await adapter.execute(`
      CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE,
        age INTEGER,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Insert test data
    await adapter.execute(
      `INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`
    );
    await adapter.execute(
      `INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)`
    );
    await adapter.execute(
      `INSERT INTO users (name, email, age) VALUES ('Charlie', 'charlie@example.com', 35)`
    );
  });

  afterEach(async () => {
    await adapter.close();
  });

  describe('basic SELECT queries', () => {
    it('should execute a simple SELECT query', async () => {
      const result = await adapter.query('SELECT * FROM users');
      expect(result.rows).toHaveLength(3);
    });

    it('should return correct row count', async () => {
      const result = await adapter.query('SELECT * FROM users');
      expect(result.rowCount).toBe(3);
    });

    it('should return rows as objects with column names as keys', async () => {
      const result = await adapter.query('SELECT name, email FROM users WHERE id = 1');
      expect(result.rows[0]).toEqual({
        name: 'Alice',
        email: 'alice@example.com',
      });
    });

    it('should handle empty result sets', async () => {
      const result = await adapter.query('SELECT * FROM users WHERE id = 999');
      expect(result.rows).toHaveLength(0);
      expect(result.rowCount).toBe(0);
    });

    it('should return field metadata', async () => {
      const result = await adapter.query('SELECT id, name, age FROM users LIMIT 1');
      expect(result.fields).toBeDefined();
      expect(result.fields).toHaveLength(3);
      expect(result.fields![0].name).toBe('id');
      expect(result.fields![1].name).toBe('name');
      expect(result.fields![2].name).toBe('age');
    });
  });

  describe('parameterized queries', () => {
    it('should execute parameterized SELECT with single parameter', async () => {
      const result = await adapter.query('SELECT * FROM users WHERE id = ?', [1]);
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Alice');
    });

    it('should execute parameterized SELECT with multiple parameters', async () => {
      const result = await adapter.query(
        'SELECT * FROM users WHERE age >= ? AND age <= ?',
        [25, 32]
      );
      expect(result.rows).toHaveLength(2);
    });

    it('should handle string parameters', async () => {
      const result = await adapter.query(
        'SELECT * FROM users WHERE email = ?',
        ['bob@example.com']
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Bob');
    });

    it('should handle null parameters', async () => {
      await adapter.execute('INSERT INTO users (name, email, age) VALUES (?, ?, ?)', [
        'NullUser',
        'null@example.com',
        null,
      ]);
      const result = await adapter.query('SELECT * FROM users WHERE age IS NULL');
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('NullUser');
    });

    it('should handle LIKE patterns with parameters', async () => {
      const result = await adapter.query('SELECT * FROM users WHERE name LIKE ?', ['%li%']);
      expect(result.rows).toHaveLength(2); // Alice and Charlie
    });
  });

  describe('query result types', () => {
    it('should return integers as numbers', async () => {
      const result = await adapter.query('SELECT id, age FROM users WHERE id = 1');
      expect(typeof result.rows[0].id).toBe('number');
      expect(typeof result.rows[0].age).toBe('number');
    });

    it('should return text as strings', async () => {
      const result = await adapter.query('SELECT name FROM users WHERE id = 1');
      expect(typeof result.rows[0].name).toBe('string');
    });

    it('should return NULL as null', async () => {
      await adapter.execute('INSERT INTO users (name, email) VALUES (?, ?)', [
        'NullAge',
        'nullage@example.com',
      ]);
      const result = await adapter.query(
        `SELECT age FROM users WHERE email = 'nullage@example.com'`
      );
      expect(result.rows[0].age).toBeNull();
    });

    it('should return REAL as numbers', async () => {
      await adapter.execute('CREATE TABLE prices (value REAL)');
      await adapter.execute('INSERT INTO prices VALUES (?)', [3.14159]);
      const result = await adapter.query('SELECT value FROM prices');
      expect(typeof result.rows[0].value).toBe('number');
      expect(result.rows[0].value).toBeCloseTo(3.14159, 5);
    });

    it('should return BLOB as Uint8Array', async () => {
      await adapter.execute('CREATE TABLE blobs (data BLOB)');
      const blob = new Uint8Array([1, 2, 3, 4, 5]);
      await adapter.execute('INSERT INTO blobs VALUES (?)', [blob]);
      const result = await adapter.query('SELECT data FROM blobs');
      expect(result.rows[0].data).toBeInstanceOf(Uint8Array);
      expect(result.rows[0].data).toEqual(blob);
    });
  });

  describe('aggregate queries', () => {
    it('should handle COUNT(*)', async () => {
      const result = await adapter.query('SELECT COUNT(*) as count FROM users');
      expect(result.rows[0].count).toBe(3);
    });

    it('should handle SUM', async () => {
      const result = await adapter.query('SELECT SUM(age) as total FROM users');
      expect(result.rows[0].total).toBe(90); // 30 + 25 + 35
    });

    it('should handle AVG', async () => {
      const result = await adapter.query('SELECT AVG(age) as avg FROM users');
      expect(result.rows[0].avg).toBe(30); // 90 / 3
    });

    it('should handle MIN/MAX', async () => {
      const result = await adapter.query(
        'SELECT MIN(age) as min_age, MAX(age) as max_age FROM users'
      );
      expect(result.rows[0].min_age).toBe(25);
      expect(result.rows[0].max_age).toBe(35);
    });

    it('should handle GROUP BY', async () => {
      await adapter.execute(
        `INSERT INTO users (name, email, age) VALUES ('Dave', 'dave@example.com', 30)`
      );
      const result = await adapter.query(
        'SELECT age, COUNT(*) as count FROM users GROUP BY age ORDER BY age'
      );
      expect(result.rows).toHaveLength(3);
      expect(result.rows.find((r) => r.age === 30)?.count).toBe(2);
    });
  });

  describe('joins', () => {
    beforeEach(async () => {
      await adapter.execute(`
        CREATE TABLE orders (
          id INTEGER PRIMARY KEY,
          user_id INTEGER,
          product TEXT,
          FOREIGN KEY (user_id) REFERENCES users(id)
        )
      `);
      await adapter.execute('INSERT INTO orders VALUES (1, 1, "Widget")');
      await adapter.execute('INSERT INTO orders VALUES (2, 1, "Gadget")');
      await adapter.execute('INSERT INTO orders VALUES (3, 2, "Gizmo")');
    });

    it('should handle INNER JOIN', async () => {
      const result = await adapter.query(`
        SELECT users.name, orders.product
        FROM users
        INNER JOIN orders ON users.id = orders.user_id
        ORDER BY orders.id
      `);
      expect(result.rows).toHaveLength(3);
      expect(result.rows[0]).toEqual({ name: 'Alice', product: 'Widget' });
    });

    it('should handle LEFT JOIN', async () => {
      const result = await adapter.query(`
        SELECT users.name, orders.product
        FROM users
        LEFT JOIN orders ON users.id = orders.user_id
        ORDER BY users.id
      `);
      expect(result.rows).toHaveLength(4); // Charlie has no orders
      expect(result.rows.find((r) => r.name === 'Charlie')?.product).toBeNull();
    });
  });

  describe('query execution info', () => {
    it('should include execution duration', async () => {
      const result = await adapter.query('SELECT * FROM users');
      expect(result.duration).toBeDefined();
      expect(typeof result.duration).toBe('number');
      expect(result.duration).toBeGreaterThanOrEqual(0);
    });
  });

  describe('query conformance to QueryResult interface', () => {
    it('should return a valid QueryResult object', async () => {
      const result: QueryResult = await adapter.query('SELECT * FROM users');
      expect(result).toHaveProperty('rows');
      expect(result).toHaveProperty('rowCount');
      expect(Array.isArray(result.rows)).toBe(true);
      expect(typeof result.rowCount).toBe('number');
    });
  });
});
