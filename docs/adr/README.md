# Architecture Decision Records

MADR-lite: one decision per file, `NNNN-slug.md`, sections Context / Options considered /
Decision / Consequences. A decision is changed by a new ADR that supersedes the old one.

| ADR | Title |
|---|---|
| [0001](0001-scope.md) | Scope and boundaries |
| [0002](0002-language-and-dependencies.md) | Language and dependencies |
| [0003](0003-ingestion-api.md) | Ingestion API and delivery semantics |
| [0004](0004-storage.md) | Storage: raw scrape archive |
| [0005](0005-metric-convention.md) | Metric convention and its publication |
| [0006](0006-sealing-and-retention.md) | Sealing, compaction and retention |
| [0007](0007-authentication.md) | Authentication before and after hanko |
| [0008](0008-read-and-status.md) | Reading and status |
| [0009](0009-testing.md) | Testing |
| [0010](0010-forbidden-for-wrong-role.md) | 403 for a valid token without the role |
| [0011](0011-stream-owner.md) | A stream belongs to the node that first pushed it |
| [0012](0012-binaries.md) | Three binaries: data CLI, datastore, query daemon |
| [0013](0013-query-document.md) | Query document: one batched request contract for the stores |
| [0014](0014-masu-query.md) | masu-query: operators, trust boundary and budgets |
| [0015](0015-remote-data-cli.md) | The data CLI queries the daemon; the datastore reads files |
