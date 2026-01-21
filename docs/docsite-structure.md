# Documentation Site Structure

Architecture and organization for the vitess.do documentation website.

---

## Navigation Structure

### Primary Navigation (Top Bar)

```
[Logo: vitess.do]  Documentation  API Reference  Examples  Pricing  [Get Started]
```

### Sidebar Navigation (Documentation Section)

```
Getting Started
  ├── Introduction
  ├── Quick Start
  ├── Installation
  └── Your First Query

Core Concepts
  ├── Architecture Overview
  ├── VTGate (Query Router)
  ├── VTTablet (Shard Executor)
  ├── VSchema (Sharding Config)
  └── Vindexes (Shard Keys)

Client SDK
  ├── Configuration
  ├── Connecting
  ├── Queries
  ├── Write Operations
  ├── Batch Operations
  ├── Transactions
  └── Error Handling

Server Components
  ├── VTGate Setup
  ├── VTTablet Durable Objects
  ├── Storage Engines
  │   ├── PGlite (PostgreSQL)
  │   └── Turso (SQLite)
  └── Cloudflare Integration

Sharding Guide
  ├── Choosing a Sharding Key
  ├── Defining VSchema
  ├── Single-Shard Queries
  ├── Scatter-Gather Queries
  ├── Cross-Shard Transactions
  └── Resharding

Migration
  ├── From Raw PGlite
  ├── From Single-Node
  ├── From Other Solutions
  └── Schema Migrations

Operations
  ├── Monitoring
  ├── Alerting
  ├── Debugging
  ├── Performance Tuning
  └── Backup & Recovery

Reference
  ├── API Reference
  ├── VSchema Reference
  ├── Error Codes
  ├── SQL Compatibility
  └── Changelog
```

---

## Page Hierarchy

### Level 1: Top-Level Sections

| Section | Path | Description |
|---------|------|-------------|
| Home | `/` | Landing page |
| Documentation | `/docs` | Main docs hub |
| API Reference | `/api` | Full API docs |
| Examples | `/examples` | Code examples |
| Pricing | `/pricing` | Pricing tiers |

### Level 2: Documentation Categories

| Category | Path | Pages |
|----------|------|-------|
| Getting Started | `/docs/getting-started` | 4 pages |
| Core Concepts | `/docs/concepts` | 5 pages |
| Client SDK | `/docs/client` | 7 pages |
| Server | `/docs/server` | 5 pages |
| Sharding | `/docs/sharding` | 6 pages |
| Migration | `/docs/migration` | 4 pages |
| Operations | `/docs/operations` | 5 pages |
| Reference | `/docs/reference` | 5 pages |

### Level 3: Individual Pages

```
/docs
├── /getting-started
│   ├── /introduction          # What is vitess.do
│   ├── /quick-start           # 5-minute tutorial
│   ├── /installation          # npm install, setup
│   └── /first-query           # Hello world query
│
├── /concepts
│   ├── /architecture          # System overview
│   ├── /vtgate                # Query routing
│   ├── /vttablet              # Shard execution
│   ├── /vschema               # Schema configuration
│   └── /vindexes              # Sharding algorithms
│
├── /client
│   ├── /configuration         # VitessConfig options
│   ├── /connecting            # connect/disconnect
│   ├── /queries               # query() method
│   ├── /writes                # execute() method
│   ├── /batch                 # batch() method
│   ├── /transactions          # transaction() method
│   └── /errors                # Error handling
│
├── /server
│   ├── /vtgate-setup          # Worker deployment
│   ├── /vttablet-do           # Durable Object setup
│   ├── /storage-engines       # Overview
│   │   ├── /pglite            # PostgreSQL backend
│   │   └── /turso             # SQLite backend
│   └── /cloudflare            # wrangler.toml, bindings
│
├── /sharding
│   ├── /choosing-key          # Key selection guide
│   ├── /vschema-config        # VSchema definition
│   ├── /single-shard          # Point queries
│   ├── /scatter-gather        # Cross-shard queries
│   ├── /transactions          # 2PC transactions
│   └── /resharding            # Adding/removing shards
│
├── /migration
│   ├── /from-pglite           # Raw PGlite migration
│   ├── /from-single-node      # Unsharded to sharded
│   ├── /from-other            # Citus, MongoDB, etc.
│   └── /schema-changes        # Online DDL
│
├── /operations
│   ├── /monitoring            # Metrics, dashboards
│   ├── /alerting              # Alert configuration
│   ├── /debugging             # Troubleshooting
│   ├── /performance           # Query optimization
│   └── /backup                # Backup strategies
│
└── /reference
    ├── /api                   # Full API reference
    ├── /vschema               # VSchema spec
    ├── /errors                # Error code reference
    ├── /sql-compat            # SQL compatibility matrix
    └── /changelog             # Version history
```

---

## Search Configuration

### Search Provider
Algolia DocSearch or similar full-text search.

### Indexing Strategy

```json
{
  "index_name": "vitess_do_docs",
  "start_urls": [
    "https://vitess.do/docs/"
  ],
  "selectors": {
    "lvl0": "nav.sidebar .active-section",
    "lvl1": "article h1",
    "lvl2": "article h2",
    "lvl3": "article h3",
    "content": "article p, article li, article code"
  },
  "custom_settings": {
    "searchableAttributes": [
      "lvl0",
      "lvl1",
      "lvl2",
      "lvl3",
      "content"
    ],
    "attributesToHighlight": [
      "lvl0",
      "lvl1",
      "lvl2",
      "lvl3",
      "content"
    ]
  }
}
```

### Search Features
- Keyboard shortcut: `/` or `Cmd+K`
- Recent searches history
- Popular searches suggestions
- Category filtering
- Code block search support

---

## Page Templates

### Documentation Page Template

```markdown
---
title: Page Title
description: Meta description for SEO
sidebar_label: Shorter Label (optional)
sidebar_position: 1
---

# Page Title

Brief introduction paragraph explaining what this page covers.

## Section Heading

Content with code examples:

```typescript
// Example code
const client = createClient({ endpoint: '...' });
```

### Subsection

More detailed content.

:::tip
Helpful tip callout
:::

:::warning
Important warning callout
:::

:::info
Informational note
:::

## Next Steps

- [Related Page 1](/docs/path)
- [Related Page 2](/docs/path)
```

### API Reference Page Template

```markdown
---
title: methodName
description: Description of method
---

# methodName

Brief description.

## Signature

```typescript
function methodName(param1: Type1, param2?: Type2): ReturnType
```

## Parameters

| Name | Type | Required | Description |
|------|------|----------|-------------|
| param1 | `Type1` | Yes | Description |
| param2 | `Type2` | No | Description |

## Returns

`ReturnType` - Description of return value.

## Example

```typescript
const result = await client.methodName(value1, value2);
```

## Errors

| Code | Description |
|------|-------------|
| `ERROR_CODE` | When this error occurs |

## See Also

- [relatedMethod](/api/relatedMethod)
```

### Example Page Template

```markdown
---
title: Example Title
description: What this example demonstrates
tags: [tag1, tag2]
---

# Example Title

## Overview

What this example demonstrates and when to use it.

## Prerequisites

- Requirement 1
- Requirement 2

## Code

```typescript
// Full working example
import { createClient } from '@dotdo/vitess';

async function main() {
  // ...
}
```

## Explanation

Step-by-step breakdown of the code.

## Try It

Link to interactive playground or CodeSandbox.

## Related Examples

- [Another Example](/examples/another)
```

---

## Component Library

### Code Blocks

```jsx
<CodeBlock language="typescript" title="example.ts">
  {`const client = createClient({ endpoint: '...' });`}
</CodeBlock>
```

Features:
- Syntax highlighting (Prism or Shiki)
- Copy button
- Line numbers (optional)
- Line highlighting
- File name header
- Language badge

### API Signature

```jsx
<ApiSignature
  method="query"
  params={[
    { name: 'sql', type: 'string', required: true },
    { name: 'params', type: 'unknown[]', required: false }
  ]}
  returns="Promise<QueryResult<T>>"
/>
```

### Type Definition

```jsx
<TypeDef name="VitessConfig">
  <Property name="endpoint" type="string" required>
    VTGate endpoint URL
  </Property>
  <Property name="keyspace" type="string">
    Default keyspace
  </Property>
</TypeDef>
```

### Callouts

```jsx
<Tip>Helpful tip content</Tip>
<Warning>Warning content</Warning>
<Info>Informational content</Info>
<Danger>Critical warning</Danger>
```

### Tabs

```jsx
<Tabs>
  <Tab label="PostgreSQL">
    PostgreSQL-specific content
  </Tab>
  <Tab label="SQLite">
    SQLite-specific content
  </Tab>
</Tabs>
```

### Interactive Playground

```jsx
<Playground
  code={`
    const client = createClient({...});
    const result = await client.query('SELECT 1');
  `}
  editable
  runnable
/>
```

---

## URL Structure

### Canonical URLs

| Pattern | Example |
|---------|---------|
| Docs page | `https://vitess.do/docs/getting-started/quick-start` |
| API page | `https://vitess.do/api#createClient` |
| Example | `https://vitess.do/examples/multi-tenant` |

### Redirects

```
/docs -> /docs/getting-started/introduction
/api -> /docs/reference/api
/guide -> /docs/getting-started
/tutorial -> /docs/getting-started/quick-start
```

### Versioning

Future consideration for version-specific docs:

```
/docs/v1/...
/docs/v2/...
/docs/latest/... (alias to current)
```

---

## Content Guidelines

### Writing Style

- Use second person ("you") for instructions
- Present tense for descriptions
- Active voice preferred
- Short paragraphs (3-4 sentences max)
- Code examples for every concept
- Real-world use cases, not abstract examples

### Code Examples

- Always runnable (no pseudo-code)
- Include necessary imports
- Show error handling where relevant
- Use TypeScript for type annotations
- Follow consistent formatting

### Terminology

| Term | Definition |
|------|------------|
| VTGate | Query router (Cloudflare Worker) |
| VTTablet | Shard executor (Durable Object) |
| VSchema | Sharding configuration schema |
| Vindex | Virtual index for shard routing |
| Keyspace | Logical database namespace |
| Shard | Horizontal partition of data |

---

## Internationalization (Future)

### Supported Languages
- English (default)
- Future: Japanese, German, Spanish, Portuguese, Chinese

### Translation Structure
```
/docs/en/getting-started/...
/docs/ja/getting-started/...
```

### Translation Workflow
1. English source of truth
2. Machine translation draft
3. Community review
4. Professional review for key pages

---

## Analytics & Feedback

### Page Analytics
- Page views
- Time on page
- Scroll depth
- Search queries
- 404 errors

### User Feedback
- "Was this helpful?" widget on each page
- Feedback form for detailed comments
- GitHub issues link for bugs

### Content Metrics
- Most visited pages
- Common search queries with no results
- Drop-off points in tutorials
- Time to first query (onboarding funnel)

---

## Build & Deploy

### Static Site Generator
Docusaurus, Next.js with MDX, or Astro.

### Build Process
```bash
# Development
npm run docs:dev

# Production build
npm run docs:build

# Deploy
npm run docs:deploy
```

### Preview Deployments
- PR previews via Cloudflare Pages
- Branch-specific URLs: `docs-feature-xyz.vitess.do`

### CI/CD
1. Lint Markdown
2. Check broken links
3. Validate code examples
4. Build static site
5. Deploy to Cloudflare Pages
