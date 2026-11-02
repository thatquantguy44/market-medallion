# Project handoff

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
neither connector is implemented. NAAIM's delayed public research feed is the
current access-gated source review; see [Spec 007](../specs/spec007/README.md).
