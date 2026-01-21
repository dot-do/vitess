/**
 * PGliteAdapter Execute Tests
 *
 * TDD Red tests for INSERT, UPDATE, DELETE operations.
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { PGliteAdapter, PGliteAdapterError, PGliteErrorCode } from '../index.js';

describe('PGliteAdapter Execute', () => {
  let adapter: PGliteAdapter;

  beforeAll(async () => {
    adapter = new PGliteAdapter();
    await adapter.init();

    // Set up test tables
    await adapter.execute(`
      CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        price DECIMAL(10, 2) NOT NULL,
        quantity INT DEFAULT 0,
        category TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await adapter.execute(`
      CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        product_id INT REFERENCES products(id) ON DELETE CASCADE,
        quantity INT NOT NULL,
        total DECIMAL(10, 2) NOT NULL
      )
    `);

    await adapter.execute(`
      CREATE TABLE unique_items (
        id SERIAL PRIMARY KEY,
        code TEXT UNIQUE NOT NULL,
        name TEXT NOT NULL
      )
    `);
  });

  afterAll(async () => {
    await adapter.close();
  });

  beforeEach(async () => {
    // Clean up test data before each test
    await adapter.execute('DELETE FROM orders');
    await adapter.execute('DELETE FROM products');
    await adapter.execute('DELETE FROM unique_items');
    // Reset sequences
    await adapter.execute('ALTER SEQUENCE products_id_seq RESTART WITH 1');
    await adapter.execute('ALTER SEQUENCE orders_id_seq RESTART WITH 1');
    await adapter.execute('ALTER SEQUENCE unique_items_id_seq RESTART WITH 1');
  });

  describe('INSERT operations', () => {
    it('should insert a single row', async () => {
      const result = await adapter.execute(
        "INSERT INTO products (name, price) VALUES ('Widget', 9.99)"
      );
      expect(result.affected).toBe(1);
    });

    it('should return lastInsertId for SERIAL columns', async () => {
      const result = await adapter.execute(
        "INSERT INTO products (name, price) VALUES ('Widget', 9.99)"
      );
      expect(result.lastInsertId).toBeDefined();
      expect(result.lastInsertId).toBe(1);

      const result2 = await adapter.execute(
        "INSERT INTO products (name, price) VALUES ('Gadget', 19.99)"
      );
      expect(result2.lastInsertId).toBe(2);
    });

    it('should insert with parameters', async () => {
      const result = await adapter.execute(
        'INSERT INTO products (name, price, quantity, category) VALUES ($1, $2, $3, $4)',
        ['Laptop', 999.99, 10, 'Electronics']
      );
      expect(result.affected).toBe(1);

      const query = await adapter.query('SELECT * FROM products WHERE name = $1', [
        'Laptop',
      ]);
      expect(query.rows[0]).toMatchObject({
        name: 'Laptop',
        price: '999.99', // DECIMAL comes back as string
        quantity: 10,
        category: 'Electronics',
      });
    });

    it('should insert multiple rows', async () => {
      const result = await adapter.execute(`
        INSERT INTO products (name, price) VALUES
        ('Item1', 10.00),
        ('Item2', 20.00),
        ('Item3', 30.00)
      `);
      expect(result.affected).toBe(3);
    });

    it('should handle INSERT with DEFAULT values', async () => {
      const result = await adapter.execute(
        "INSERT INTO products (name, price) VALUES ('Default Test', 5.00)"
      );
      expect(result.affected).toBe(1);

      const query = await adapter.query(
        "SELECT quantity, created_at FROM products WHERE name = 'Default Test'"
      );
      expect(query.rows[0].quantity).toBe(0); // DEFAULT 0
      expect(query.rows[0].created_at).toBeDefined(); // DEFAULT CURRENT_TIMESTAMP
    });

    it('should handle INSERT with NULL values', async () => {
      const result = await adapter.execute(
        'INSERT INTO products (name, price, category) VALUES ($1, $2, $3)',
        ['No Category', 15.00, null]
      );
      expect(result.affected).toBe(1);

      const query = await adapter.query(
        "SELECT category FROM products WHERE name = 'No Category'"
      );
      expect(query.rows[0].category).toBeNull();
    });

    it('should handle INSERT RETURNING', async () => {
      // Note: execute returns ExecuteResult, not QueryResult
      // For RETURNING, use query instead
      const result = await adapter.query(
        "INSERT INTO products (name, price) VALUES ('ReturnTest', 25.00) RETURNING id, name"
      );
      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].id).toBeDefined();
      expect(result.rows[0].name).toBe('ReturnTest');
    });

    it('should throw on NOT NULL constraint violation', async () => {
      await expect(
        adapter.execute('INSERT INTO products (name, price) VALUES (NULL, 10.00)')
      ).rejects.toThrow(PGliteAdapterError);
      await expect(
        adapter.execute('INSERT INTO products (name, price) VALUES (NULL, 10.00)')
      ).rejects.toMatchObject({
        code: PGliteErrorCode.CONSTRAINT_VIOLATION,
      });
    });

    it('should throw on UNIQUE constraint violation', async () => {
      await adapter.execute(
        "INSERT INTO unique_items (code, name) VALUES ('ABC123', 'First')"
      );

      await expect(
        adapter.execute("INSERT INTO unique_items (code, name) VALUES ('ABC123', 'Second')")
      ).rejects.toThrow(PGliteAdapterError);
      await expect(
        adapter.execute("INSERT INTO unique_items (code, name) VALUES ('ABC123', 'Second')")
      ).rejects.toMatchObject({
        code: PGliteErrorCode.CONSTRAINT_VIOLATION,
      });
    });

    it('should throw on FOREIGN KEY constraint violation', async () => {
      await expect(
        adapter.execute('INSERT INTO orders (product_id, quantity, total) VALUES (999, 1, 10.00)')
      ).rejects.toThrow(PGliteAdapterError);
      await expect(
        adapter.execute('INSERT INTO orders (product_id, quantity, total) VALUES (999, 1, 10.00)')
      ).rejects.toMatchObject({
        code: PGliteErrorCode.CONSTRAINT_VIOLATION,
      });
    });

    it('should handle INSERT ON CONFLICT DO NOTHING', async () => {
      await adapter.execute(
        "INSERT INTO unique_items (code, name) VALUES ('DUP', 'Original')"
      );

      const result = await adapter.execute(
        "INSERT INTO unique_items (code, name) VALUES ('DUP', 'Duplicate') ON CONFLICT DO NOTHING"
      );
      expect(result.affected).toBe(0);

      const query = await adapter.query("SELECT name FROM unique_items WHERE code = 'DUP'");
      expect(query.rows[0].name).toBe('Original');
    });

    it('should handle INSERT ON CONFLICT DO UPDATE (upsert)', async () => {
      await adapter.execute(
        "INSERT INTO unique_items (code, name) VALUES ('UPSERT', 'Original')"
      );

      const result = await adapter.execute(
        "INSERT INTO unique_items (code, name) VALUES ('UPSERT', 'Updated') ON CONFLICT (code) DO UPDATE SET name = EXCLUDED.name"
      );
      expect(result.affected).toBe(1);

      const query = await adapter.query("SELECT name FROM unique_items WHERE code = 'UPSERT'");
      expect(query.rows[0].name).toBe('Updated');
    });
  });

  describe('UPDATE operations', () => {
    beforeEach(async () => {
      // Insert test data
      await adapter.execute(
        "INSERT INTO products (name, price, quantity, category) VALUES ('Widget', 10.00, 100, 'Tools')"
      );
      await adapter.execute(
        "INSERT INTO products (name, price, quantity, category) VALUES ('Gadget', 20.00, 50, 'Electronics')"
      );
      await adapter.execute(
        "INSERT INTO products (name, price, quantity, category) VALUES ('Gizmo', 30.00, 25, 'Electronics')"
      );
    });

    it('should update a single row', async () => {
      const result = await adapter.execute(
        "UPDATE products SET price = 15.00 WHERE name = 'Widget'"
      );
      expect(result.affected).toBe(1);

      const query = await adapter.query("SELECT price FROM products WHERE name = 'Widget'");
      expect(query.rows[0].price).toBe('15.00');
    });

    it('should update multiple rows', async () => {
      const result = await adapter.execute(
        "UPDATE products SET category = 'Tech' WHERE category = 'Electronics'"
      );
      expect(result.affected).toBe(2);

      // PGlite returns number for COUNT (not bigint like native Postgres)
      const query = await adapter.query("SELECT COUNT(*) as count FROM products WHERE category = 'Tech'");
      expect(query.rows[0].count).toBe(2);
    });

    it('should update with parameters', async () => {
      const result = await adapter.execute(
        'UPDATE products SET price = $1, quantity = $2 WHERE name = $3',
        [99.99, 200, 'Widget']
      );
      expect(result.affected).toBe(1);

      const query = await adapter.query("SELECT price, quantity FROM products WHERE name = 'Widget'");
      expect(query.rows[0]).toMatchObject({ price: '99.99', quantity: 200 });
    });

    it('should return 0 affected when no rows match', async () => {
      const result = await adapter.execute(
        "UPDATE products SET price = 100.00 WHERE name = 'NonExistent'"
      );
      expect(result.affected).toBe(0);
    });

    it('should handle UPDATE with subquery', async () => {
      // Insert an order
      await adapter.execute(
        'INSERT INTO orders (product_id, quantity, total) VALUES (1, 5, 50.00)'
      );

      const result = await adapter.execute(`
        UPDATE products
        SET quantity = quantity - (SELECT quantity FROM orders WHERE product_id = products.id)
        WHERE id IN (SELECT product_id FROM orders)
      `);
      expect(result.affected).toBe(1);

      const query = await adapter.query('SELECT quantity FROM products WHERE id = 1');
      expect(query.rows[0].quantity).toBe(95); // 100 - 5
    });

    it('should handle UPDATE with RETURNING', async () => {
      const result = await adapter.query(
        "UPDATE products SET price = price * 1.1 WHERE category = 'Electronics' RETURNING id, name, price"
      );
      expect(result.rows).toHaveLength(2);
      result.rows.forEach((row) => {
        expect(row.id).toBeDefined();
        expect(row.name).toBeDefined();
        expect(row.price).toBeDefined();
      });
    });

    it('should throw on constraint violation during UPDATE', async () => {
      await expect(
        adapter.execute("UPDATE products SET name = NULL WHERE name = 'Widget'")
      ).rejects.toThrow(PGliteAdapterError);
    });
  });

  describe('DELETE operations', () => {
    beforeEach(async () => {
      // Insert test data
      await adapter.execute(
        "INSERT INTO products (name, price, category) VALUES ('Delete1', 10.00, 'A')"
      );
      await adapter.execute(
        "INSERT INTO products (name, price, category) VALUES ('Delete2', 20.00, 'A')"
      );
      await adapter.execute(
        "INSERT INTO products (name, price, category) VALUES ('Delete3', 30.00, 'B')"
      );
    });

    it('should delete a single row', async () => {
      const result = await adapter.execute("DELETE FROM products WHERE name = 'Delete1'");
      expect(result.affected).toBe(1);

      // PGlite returns number for COUNT (not bigint like native Postgres)
      const query = await adapter.query('SELECT COUNT(*) as count FROM products');
      expect(query.rows[0].count).toBe(2);
    });

    it('should delete multiple rows', async () => {
      const result = await adapter.execute("DELETE FROM products WHERE category = 'A'");
      expect(result.affected).toBe(2);

      // PGlite returns number for COUNT (not bigint like native Postgres)
      const query = await adapter.query('SELECT COUNT(*) as count FROM products');
      expect(query.rows[0].count).toBe(1);
    });

    it('should delete all rows with no WHERE clause', async () => {
      const result = await adapter.execute('DELETE FROM products');
      expect(result.affected).toBe(3);

      // PGlite returns number for COUNT (not bigint like native Postgres)
      const query = await adapter.query('SELECT COUNT(*) as count FROM products');
      expect(query.rows[0].count).toBe(0);
    });

    it('should delete with parameters', async () => {
      const result = await adapter.execute(
        'DELETE FROM products WHERE name = $1',
        ['Delete2']
      );
      expect(result.affected).toBe(1);
    });

    it('should return 0 affected when no rows match', async () => {
      const result = await adapter.execute("DELETE FROM products WHERE name = 'NonExistent'");
      expect(result.affected).toBe(0);
    });

    it('should handle DELETE with RETURNING', async () => {
      const result = await adapter.query(
        "DELETE FROM products WHERE category = 'A' RETURNING id, name"
      );
      expect(result.rows).toHaveLength(2);
    });

    it('should cascade delete with ON DELETE CASCADE', async () => {
      // Insert a product and an order
      await adapter.execute(
        "INSERT INTO products (name, price) VALUES ('CascadeTest', 50.00)"
      );
      const productQuery = await adapter.query(
        "SELECT id FROM products WHERE name = 'CascadeTest'"
      );
      const productId = productQuery.rows[0].id;

      await adapter.execute(
        'INSERT INTO orders (product_id, quantity, total) VALUES ($1, 1, 50.00)',
        [productId]
      );

      // Delete the product - order should be cascade deleted
      await adapter.execute('DELETE FROM products WHERE id = $1', [productId]);

      // PGlite returns number for COUNT (not bigint like native Postgres)
      const orderQuery = await adapter.query(
        'SELECT COUNT(*) as count FROM orders WHERE product_id = $1',
        [productId]
      );
      expect(orderQuery.rows[0].count).toBe(0);
    });
  });

  describe('DDL operations', () => {
    afterEach(async () => {
      // Cleanup any test tables
      await adapter.execute('DROP TABLE IF EXISTS test_ddl CASCADE');
      await adapter.execute('DROP TABLE IF EXISTS test_alter CASCADE');
    });

    it('should CREATE TABLE', async () => {
      const result = await adapter.execute(`
        CREATE TABLE test_ddl (
          id SERIAL PRIMARY KEY,
          value TEXT
        )
      `);
      expect(result.affected).toBe(0); // DDL doesn't affect rows

      const query = await adapter.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'test_ddl'"
      );
      expect(query.rows).toHaveLength(1);
    });

    it('should DROP TABLE', async () => {
      await adapter.execute('CREATE TABLE test_ddl (id INT)');
      await adapter.execute('DROP TABLE test_ddl');

      const query = await adapter.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'test_ddl'"
      );
      expect(query.rows).toHaveLength(0);
    });

    it('should ALTER TABLE ADD COLUMN', async () => {
      await adapter.execute('CREATE TABLE test_alter (id INT)');
      await adapter.execute('ALTER TABLE test_alter ADD COLUMN name TEXT');

      const query = await adapter.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_alter'"
      );
      expect(query.rows.map((r) => r.column_name)).toContain('name');
    });

    it('should CREATE INDEX', async () => {
      await adapter.execute('CREATE TABLE test_ddl (id INT, value TEXT)');
      await adapter.execute('CREATE INDEX idx_test_value ON test_ddl (value)');

      const query = await adapter.query(
        "SELECT indexname FROM pg_indexes WHERE tablename = 'test_ddl'"
      );
      expect(query.rows.map((r) => r.indexname)).toContain('idx_test_value');
    });

    it('should handle CREATE TABLE IF NOT EXISTS', async () => {
      await adapter.execute('CREATE TABLE test_ddl (id INT)');

      // Should not throw
      await adapter.execute('CREATE TABLE IF NOT EXISTS test_ddl (id INT)');
    });

    it('should handle DROP TABLE IF EXISTS', async () => {
      // Should not throw even if table doesn't exist
      await adapter.execute('DROP TABLE IF EXISTS nonexistent_table');
    });
  });

  describe('error handling', () => {
    it('should throw on syntax error', async () => {
      await expect(
        adapter.execute('INSERTT INTO products VALUES (1)')
      ).rejects.toThrow(PGliteAdapterError);
      await expect(
        adapter.execute('INSERTT INTO products VALUES (1)')
      ).rejects.toMatchObject({
        code: PGliteErrorCode.SYNTAX_ERROR,
      });
    });

    it('should throw when adapter is not ready', async () => {
      const newAdapter = new PGliteAdapter();
      await expect(
        newAdapter.execute('INSERT INTO products (name, price) VALUES ($1, $2)', ['Test', 10])
      ).rejects.toThrow(PGliteAdapterError);
      await expect(
        newAdapter.execute('INSERT INTO products (name, price) VALUES ($1, $2)', ['Test', 10])
      ).rejects.toMatchObject({
        code: PGliteErrorCode.NOT_READY,
      });
    });

    it('should throw when adapter is closed', async () => {
      const newAdapter = new PGliteAdapter();
      await newAdapter.init();
      await newAdapter.close();

      await expect(
        newAdapter.execute('INSERT INTO test VALUES (1)')
      ).rejects.toThrow(PGliteAdapterError);
      await expect(
        newAdapter.execute('INSERT INTO test VALUES (1)')
      ).rejects.toMatchObject({
        code: PGliteErrorCode.ALREADY_CLOSED,
      });
    });

    it('should include cause error', async () => {
      try {
        await adapter.execute('INSERT INTO products (name, price) VALUES (NULL, 10)');
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PGliteAdapterError);
        expect((error as PGliteAdapterError).cause).toBeDefined();
      }
    });
  });

  describe('concurrent writes', () => {
    it('should handle concurrent inserts', async () => {
      const promises = Array.from({ length: 10 }, (_, i) =>
        adapter.execute('INSERT INTO products (name, price) VALUES ($1, $2)', [
          `Product${i}`,
          i * 10,
        ])
      );

      const results = await Promise.all(promises);

      results.forEach((result) => {
        expect(result.affected).toBe(1);
      });

      // PGlite returns number for COUNT (not bigint like native Postgres)
      const count = await adapter.query('SELECT COUNT(*) as count FROM products');
      expect(count.rows[0].count).toBe(10);
    });

    it('should serialize writes correctly', async () => {
      // Insert initial value
      await adapter.execute(
        "INSERT INTO products (name, price, quantity) VALUES ('Counter', 0, 0)"
      );

      // Concurrent increments
      const promises = Array.from({ length: 5 }, () =>
        adapter.execute(
          "UPDATE products SET quantity = quantity + 1 WHERE name = 'Counter'"
        )
      );

      await Promise.all(promises);

      const result = await adapter.query(
        "SELECT quantity FROM products WHERE name = 'Counter'"
      );
      expect(result.rows[0].quantity).toBe(5);
    });
  });
});
