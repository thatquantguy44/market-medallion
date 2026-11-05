# Project Agent Guidance

Read `README.md`, `docs/pipelines.md`, `specs/spec001/README.md`, and
`docs/handoffs/project_bootstrap.md` before substantive work. Keep implemented
slices distinct from planned connectors; use the current handoff for status.
For the AAII slice, also read `specs/spec002/README.md`, `specs/spec003/README.md`, and
`docs/handoffs/aaii_checkpoint.md`. The checkpoint defines the next bounded scope;
do not restart from Spec 001's original CFTC-first plan.

Read `agents/README.md` for the available agent libraries and task routing:

- **agent-skills** is the repository-local snapshot at `agents/agent-skills/`,
  pinned in `agents/agent-skills.lock.json`. For non-trivial engineering work,
  first read `skills/using-agent-skills/SKILL.md` inside that snapshot, then only
  the workflow skills it routes to. Verify with
  `python3 scripts/verify_agent_skills.py`; no refresh or initialization is needed.
- **QuantSmith** is available through `agents/quantsmith.reference.json`, a
  revision/checksum pin to the existing local checkout. Resolve/verify with
  `python3 scripts/verify_quantsmith.py`; override its configured location with
  `MARKET_MEDALLION_QUANTSMITH_PATH` or `--path`. For quant/data/provider/pipeline
  work, also read its engineering principles, spec-driven workflow and agent
  catalog, then use the relevant pinned specialist instructions listed in
  `agents/README.md`. It should be leveraged where its expertise fits the task.

User and repository instructions/specs take precedence over library guidance.
Keep this repository's spec conventions, overlapping revision refreshes and
capture-time first-seen semantics when generic recipes differ. QuantSmith is a
read-only external guidance reference; its repo-local hooks/setup and its own
agent-skills bundle do not replace this repository's setup. The agent-skills
root `AGENTS.md` governs work inside that vendored snapshot only.

Preserve the manifest-driven medallion layout and pure-Python core. Characterize
behavior with offline fixtures before changes to data semantics. Preserve raw
payloads, identifiers, publication timing, and observed revisions. Verify
provider schemas and access policies from official documentation before coding.

Keep this project independent of sibling repositories. Never copy their secrets,
runtime config, databases, Git history, or deployment identifiers. Do not edit
vendored files; pin upgrades in a dedicated reviewed change following
`docs/development/agent_library.md`.

Every implemented pipeline MUST own a separate snake_case subpackage under
`src/market_data_pipeline/pipelines/<pipeline_name>/`. AAII lives in
`src/market_data_pipeline/pipelines/aaii/`. Create all future pipeline
implementations in this `pipelines/` namespace; do not add provider packages
directly under `src/market_data_pipeline/`. Keep provider schemas, manifests,
acquisition, warehouse adapters, operations and Dagster assets inside that pipeline. Use `shared/`
only for provider-independent helpers. Read `docs/development/pipeline_layout.md`
for import changes. Update the master register `docs/pipelines.md` whenever a
pipeline's scope, package, implementation or deployment status changes.

Outbound HTTP must use the run-scoped settings/client in `shared/http.py`.
Read `docs/development/outbound_http.md`: select/probe once after provider access
gates, reuse that client for the run, preserve source-owned retries and raw
capture, and keep local/replay/health paths free of network probes. Future HTTP
adapters must expose the same pipeline-owned `network` settings. Never install
a process-global opener or log proxy URLs/credentials.
