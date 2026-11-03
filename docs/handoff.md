# Project handoff

**Current priority:** the [five-stage securities-finance program](securities_finance/README.md)
and [delivery roadmap](securities_finance/roadmap.md). Spec011's offline SEC
parser/manifest reconciles both approved September2026 archives. Continue
S11-T05 observed revisions/reversions and withdrawals. T03/T04 immutable
Bronze, root-bound SQLite and Gold are complete; see the
[runbook](deployment/sec_ftd_runbook.md). T18 types/serialization are complete for both sources.
PostgreSQL/Databricks adapters and parity tests in
T19–T22 are required delivery work, not implemented yet. See its
[checkpoint](../specs/spec011/source_review.md).
NAAIM is paused; do not resume its capture/schedule without the
user's explicit direction.

Start with the [master handoff](handoffs/project_bootstrap.md), then the
[AAII checkpoint](handoffs/aaii_checkpoint.md) for current implementation,
local service/data state and the next scoped source task.

- [Master pipeline register](pipelines.md): implementation status and ownership.
- [Dagster job and schedule register](deployment/dagster_jobs.md): active jobs,
  schedule state, runtime configuration and operating controls.
- [Specification index](../specs/README.md): accepted scopes and future contracts.
- [Agent libraries](../agents/README.md): pinned agent-skills and QuantSmith guidance.
- [Verification](development/verification.md): test/build evidence and operational limits.

AAII is implemented locally; recurring acquisition and production deployment
remain gated. CFTC has a provider contract and web traffic remains a draft;
neither connector is implemented. NAAIM's delayed source-owned research feed
can now publish its local, attributed `naaim-exposure-research/1` artifact for
the separately owned QuantMeridian research display; see
[Spec 007](../specs/spec007/README.md). It remains a three-month-delayed local
research capability, not a public service or current-data feed.
