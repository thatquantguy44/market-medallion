# Available agent libraries

This is the discovery document for libraries available to MarketMedallion agents.
Read the root [AGENTS.md](../AGENTS.md) first. Use both libraries where relevant:
agent-skills supplies engineering workflows; QuantSmith supplies quant/data
standards, specialist roles and review prompts. These are development resources,
not pipeline data sources or automatic runtime dependencies.

| Library | Availability | Pin / configuration | Start here |
| --- | --- | --- | --- |
| **agent-skills** | Complete repository-local snapshot at `agents/agent-skills/`; works offline on fresh clones | [`agent-skills.lock.json`](agent-skills.lock.json), revision `f63ec56a3cc936408d792956ae583c3c96a825bd` | [`skills/using-agent-skills/SKILL.md`](agent-skills/skills/using-agent-skills/SKILL.md) |
| **QuantSmith** | Configurable reference to the existing local checkout; its files stay there | [`quantsmith.reference.json`](quantsmith.reference.json), revision `41c56458f476a6c32fa922f7b8c70bb92e4fd21e`, 61 pinned guidance-file hashes | Resolve/verify below, then read the listed standards and relevant agent role |

## QuantSmith location and verification

The configured default is the user-provided local checkout:

```text
/Users/joshualutkemuller/Documents/Quant Sandbox/agentic_workflows/qf_workflow_sdk_public/
```

`agents/quantsmith.reference.json` records this path, the override environment
variable, exact Git revision/tree and SHA-256 digests of the selected guidance.
Resolve and verify it from the repository with:

```bash
python3 scripts/verify_quantsmith.py
```

The command returns the resolved path, verified revision, guidance-file count
and whether other working-tree changes exist. It reads local Git/files only.
No GitHub refresh, checkout, pull, installation or external repository write occurs.
It can also run from another working directory using its absolute script path.

On another machine, or after moving the library, override the path:

```bash
export MARKET_MEDALLION_QUANTSMITH_PATH="/path/to/local/QuantSmith"
python3 scripts/verify_quantsmith.py
# An explicit command-line override takes precedence over the environment:
python3 scripts/verify_quantsmith.py --path "/another/local/QuantSmith"
```

Resolution order is **`--path` → `MARKET_MEDALLION_QUANTSMITH_PATH` → reference
`default_path`**. Point at the checkout root, not `src/quantsmith/`. A missing
checkout, different revision/tree or changed pinned guidance returns exit 1.
Relocation does not change the revision pin. There is no automatic fallback to
a floating GitHub branch, installed package or QuantSmith's separate agent-skills bundle.

This is a **local-reference pin**, not a vendored QuantSmith copy or a pin of its
entire working tree/runtime environment. The selected 61 files matched committed
bytes when registered. Unrelated uncommitted files are reported and left alone;
they are outside this guidance pin. The provided checkout already had unrelated
work in progress. Do not assume those drafts or unpinned runtime files are
reproducible under this reference. agent-skills remains the fully vendored library.

## How agents should use the libraries

For non-trivial engineering, start with agent-skills' discovery skill and load
only the relevant workflow skills. For quant/data/provider/pipeline work, also
verify QuantSmith and read these files relative to the returned path:

1. `instructions/engineering_principles.md` — specification, reproducibility,
   reversibility, observability, honest reporting and data handling.
2. `instructions/spec_driven_development.md` — traceable acceptance and evidence;
   retain this repository's existing numbered-spec conventions.
3. `agents/README.md` — role catalog and routing.
4. Select the relevant role below; read its `README.md`, `instructions.md` and
   only the prompt/task material needed. The four documents for each listed role
   are included in the guidance hash inventory.

`README.md` provides the SDK's current overview and `CLAUDE.md` provides its
contributor/workflow context. Its repo-local setup, hook, release and agent-export
instructions govern work in QuantSmith; they do not install themselves here.
Reading a role is a way to use its expertise; it does not require spawning
another agent or running its reference runtime.

| Work in MarketMedallion | QuantSmith role under `agents/` |
| --- | --- |
| Scope and workflow handoffs | `workflow_orchestrator/` |
| Provider HTTP APIs, downloads and pagination | `data_ingestion/api_ingestion/` |
| CSV, XLS and Parquet ingestion | `data_ingestion/file_ingestion/` |
| PostgreSQL/warehouse connectivity | `data_ingestion/database_connectivity/` |
| DAGs, contracts, retries and idempotency | `data_engineering/pipeline_orchestration/` |
| Freshness, run evidence and operational monitoring | `data_engineering/pipeline_observability/` |
| Warehouse grain and key design | `data_engineering/data_modeling/` |
| Historical replay and restatements | `data_engineering/backfill_reprocessing/` |
| Provider schema drift | `data_engineering/schema_evolution/` |
| Data quality and timestamp/coverage review | `data_quality/` |
| Raw lineage and observed revisions | `provenance/lineage_capture/`, `provenance/bitemporal_data/` |
| Python verification | `test_engineering/python_test_engineer/` |

The pin also covers `instructions/pipeline_engineering.md`,
`instructions/point_in_time.md`, `instructions/reproducibility.md` and
`instructions/data_provenance.md`. Use those standards for matching tasks.
Additional QuantSmith roles or runtime integration require checking/reviewing
their own files and expanding the pin deliberately when adopting them.

## Project contracts take precedence

User and repository instructions/specs take precedence over library recipes.
Keep provider-specific grain, access policy and publication/retrieval/first-seen
fields separate. In particular:

- MarketMedallion refreshes overlapping dates to discover corrections; a generic
  “skip completed partitions” recipe must not suppress observed revisions.
- When publication vintages are unavailable, historical backfill first-seen is
  actual capture time. Keep unknown publication time null; QuantSmith's known-time
  examples do not justify fabricating older availability.
- Use this repository's pinned agent-skills snapshot; QuantSmith's own vendored
  subset and precedence rules do not replace it.

Use QuantSmith as read-only guidance here. Keep this project's secrets, runtime
configuration, data, Git history and deployment state independent. Copying code,
installing SDK runtimes/hooks or adopting provider access is a separate scoped
integration. If a library cannot be resolved or verified, report that accurately
and continue work that does not depend on it; do not silently refresh the pin.

## Pin maintenance

```bash
python3 scripts/verify_agent_skills.py
python3 scripts/verify_quantsmith.py
```

For agent-skills, follow the existing
[snapshot upgrade procedure](../docs/development/agent_library.md).
For QuantSmith, deliberately select/review a revision in the external checkout,
verify adopted guidance has no uncommitted changes, then update the reference's
revision/tree and selected-file digests together. Review changed guidance and
rerun verification/reference tests. Path-only changes need no repin if the same
revision and guidance remain available. No update-on-start mechanism is enabled.
