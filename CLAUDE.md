# Vitess.do - Claude Code Instructions

## Project Overview

Vitess.do is a Vitess-style distributed sharding solution for Cloudflare Workers/Durable Objects. It supports **both PostgreSQL (PGlite) and SQLite (Turso)** backends through a unified client SDK.

## Package Structure

```
packages/
├── vitess/          # @dotdo/vitess - Main SDK (client + server)
├── vitess-rpc/      # @dotdo/vitess-rpc - CapnWeb RPC protocol & types
├── vitess-postgres/ # @dotdo/vitess-postgres - PGlite storage engine
└── vitess-sqlite/   # @dotdo/vitess-sqlite - Turso storage engine
```

## Architecture

- **VTGate** (Worker): Query router, aggregation engine
- **VTTablet** (Durable Object): Shard manager with embedded storage
- **StorageEngine**: Pluggable backends (PGlite or Turso)
- **VSchema**: Sharding configuration (vindexes, table definitions)

## Key Concepts

### Storage Engine Abstraction

The system is backend-agnostic. The `StorageEngine` interface abstracts:
- PostgreSQL via PGlite WASM (`@dotdo/vitess-postgres`)
- SQLite via Turso/libSQL (`@dotdo/vitess-sqlite`)

### CapnWeb RPC

Client-server communication uses CapnWeb RPC protocol. Types are defined in `@dotdo/vitess-rpc`.

### Vindexes

Shard routing uses Vitess-style vindexes:
- `hash` - Hash-based sharding
- `consistent_hash` - Consistent hashing
- `range` - Range-based sharding
- `lookup` - Lookup table for routing

## NPM Package Convention

All packages use the `@dotdo/` scope:

```
@dotdo/vitess          # Main SDK
@dotdo/vitess-rpc      # RPC protocol
@dotdo/vitess-postgres # Postgres storage
@dotdo/vitess-sqlite   # SQLite storage
```

Version: `0.1.0-rc.{n}` for release candidates

## Development Commands

```bash
npm install           # Install dependencies
npm run build         # Build all packages
npm test              # Run tests
npm run typecheck     # Type check
```

## Related Repositories

- `dot-do/pocs` - POC implementations and spikes
- `dot-do/platform` - Main platform monorepo

## Beads Workflow

Use `bd` commands to track issues:

```bash
bd ready              # Show available work
bd create --title="..." --type=task --priority=2
bd update <id> --status=in_progress
bd close <id>
bd sync               # Sync at session end
```

### Hierarchical IDs for Epics

Beads supports hierarchical IDs for organizing work:

```
vitess-a3f8       (Epic)
vitess-a3f8.1     (Task under epic)
vitess-a3f8.1.1   (Sub-task)
```

Use hierarchical IDs to maintain context across sessions and track dependencies.

## Code Standards

- TypeScript strict mode
- ESM modules only
- No runtime type assertions in hot paths
- Prefer composition over inheritance
- Test with vitest
