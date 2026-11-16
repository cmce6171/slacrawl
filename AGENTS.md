# Slacrawl agent guidance

Read `CONTRIBUTING.md` for development checks. Source configuration and admission
contracts live in `docs/configuration.md`; purge behavior lives in
`docs/retention.md`.

## PR review

- Trace workspace identity and source admission through every changed ingestion,
  import, query, or sharing path. Data from one workspace must not be admitted
  under another workspace's identity or outside the selected source policy.
- Incomplete pagination, rejected pages, and unresolved history or thread work
  cannot advance successful coverage or turn an incomplete scan into success.
  Check persisted checkpoints and the status reported to consumers together.
- Preserve purge's preview/execute distinction, workspace scope, retention
  floors, and referenced-media safety. Check restoration behavior against the
  documented explicit full-sync, import, and restore routes.

Report actionable defects with the triggering conditions, affected consumer,
and code evidence. A review with no findings is valid. Use synthetic fixtures
and temporary archives; review does not run live collection, purge, or sharing.
