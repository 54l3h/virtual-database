# NDJSON Database Engine

A custom database engine built from scratch using **NestJS** and **TypeScript**.
It is built around a custom SQL-like **DSL**: a hand-written tokenizer and parser that convert raw query strings into an AST, so the engine only understands a fixed, purpose-built set of commands rather than parsing a general-purpose language.

It stores data in `.ndjson` files and uses a custom indexing system to achieve high performance without relying on any external database software.

The engine performs **direct file system access** using byte offsets, allowing precise reads and writes without loading entire files into memory.

---

## Core Features

- **Custom SQL-like DSL**
  A hand-written tokenizer and recursive-descent parser convert raw query strings into an **AST (Abstract Syntax Tree)** for safe and structured execution.

- **Smart Indexing**
  Supports **Primary Keys** and **Unique columns** using index files that store:
  - `value`
  - `offset`
  - `length`

- **Direct File Access**
  Uses `fs.FileHandle` to jump directly to the required bytes in `.ndjson` files.

- **Metadata Management**
  Automatically handles:
  - `AUTO_INCREMENT`
  - `TIMESTAMP`
  - `NOW()` logic (ISO format)

- **Soft Deletes**
  Rows are marked as `deleted: true` instead of being physically removed, preserving file offsets and index integrity.

---

## API Endpoints

The engine exposes two main endpoints to execute database operations.

---

### 1. `POST /execute/ddl`

**DDL (Data Definition Language)**
Used to define and manage database structure.

**Supported Commands**

- `CREATE TABLE`
- `CREATE DATABASE`
- `ALTER TABLE`
- `DROP TABLE`
- `DROP DATABASE`

**Example**

```json
{
  "query": "CREATE TABLE users (id SERIAL PRIMARY KEY, username TEXT, email VARCHAR(255) UNIQUE)"
}
```

### 2. `POST /execute/dml`

**DML (Data Manipulation Language)**
Used to manage the data stored inside tables.

**Supported Commands**

- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`

**Example**

```json
{
  "query": "SELECT username, email FROM users WHERE id = 1;"
}
```
