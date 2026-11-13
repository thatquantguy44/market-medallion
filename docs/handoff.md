# Project handoff

**Current priority:** the [five-stage securities-finance program](securities_finance/README.md)
and [delivery roadmap](securities_finance/roadmap.md). Spec011's offline SEC
parser/manifest reconciles both approved September2026 archives. Continue
S11-T22 threshold Delta publication. SEC T03–T07/T19/T21, threshold T20 and
shared T18 are complete; see the [runbook](deployment/sec_ftd_runbook.md).
Threshold Delta parity and final provider-use acceptance remain open. See the
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

## Presidential research local handoff

The spec 016 feature branches contain the pipeline, PRES integration and
research-site transition. The frozen archive has matched a fresh donor rebuild
and passed browser replay; see [acceptance](../specs/spec016/acceptance.md) and
[runbook](../specs/spec016/runbook.md). The feature branches are not merged, and
no live source or schedule has been enabled.
