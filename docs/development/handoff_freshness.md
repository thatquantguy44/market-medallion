# Objective master-handoff freshness

The canonical [current panel](../handoffs/project_bootstrap.md#agent-start-here)
remains a human/agent-reviewed handoff. `scripts/verify_handoff.py` makes its
coverage verifiable instead of accepting a changed timestamp as evidence.
It uses Python's standard library and local Git only; no provider request,
credential loading, runtime probe, Git mutation or automatic narrative rewrite.

## What fails the check

- Any tracked input changed without matching review metadata. The fingerprint
  includes path, mode and Git blob/commit identity for every tracked entry,
  including new/deleted/renamed files. Only the master document and its metadata
  are excluded from that fingerprint to avoid a self-reference; the entire
  document and its normalized current panel have separate fingerprints.
- Source/config/spec/test changes with no meaningful current-panel change versus
  the comparison snapshot. Date-only, comment-only, whitespace-only and historical
  appendix changes do not satisfy this condition. A concise review note can
  explain why priorities remain unchanged after a bounded change.
- A next task that differs from the first unfinished task in the selected
  canonical `todo.md`, or contradicts the panel's top priority/exact pickup.
- Missing/duplicate metadata, missing panel sections, broken local links/anchors,
  nonexistent recorded commits or a reviewed commit outside the checked history.
- Review older than seven days or dated more than five minutes into the future.
  A quiet repository still needs a real review; the job does not automatically
  advance the timestamp to hide a failure.
- Shallow history or unresolved index conflicts. Fetch complete history first.

## Local review and commit flow

1. Review the changed work, task checklist, priorities, PR/merge status and limits.
   Update the current panel and owning specs/registers as needed.
2. Stage all intended source/docs changes, including additions/deletions.
3. Refresh the evidence for that staged snapshot:

   ```bash
   python3 scripts/verify_handoff.py --refresh \
     --summary "Reviewed the completed change, acceptance evidence and next task"
   git add docs/handoffs/project_bootstrap.md docs/handoffs/handoff_state.json
   python3 scripts/verify_handoff.py --staged
   ```

4. Commit normally. The checker never stages files or edits user hooks/config.
   Existing global pre-commit hooks remain unchanged. The `--staged` command can
   also be called from an existing hook under the operator's hook policy.

`--refresh` refuses to overwrite unstaged handoff/metadata drafts or symlinks.
It writes only the document's update date and review metadata after validation.
It does not infer new priorities or mark tasks complete. It acknowledges the
**index**, not unstaged/untracked working changes; stage intended files first.
The default check reads committed HEAD. `--ref <commit>` selects another commit;
`--base <commit>` selects its comparison snapshot. Otherwise index checks compare
against HEAD and commit checks against the first parent. PR CI uses the PR base,
so a merge commit with an already reviewed tree does not require a circular
reference to its own SHA. Review provenance records the pre-commit HEAD plus
exact content fingerprints, never an invented future commit hash.

When moving to the next approved workstream, pass its owning task file on refresh,
for example `--task-file specs/spec013/todo.md`, after updating the human panel.
The initial bootstrap also requires `--task-file`; later reviews reuse it.
All check failures return1 with repair instructions. Success returns JSON with
FRESH, the actual checked commit/view, next task, review time and source fingerprint.

## CI and daily supervision

The existing `verify` job runs the guard before installing dependencies. A separate
**Handoff freshness** workflow runs on pushes, PRs, manual dispatch and daily at
12:37 UTC. Both use complete Git history and a PR's real base when applicable.
The daily workflow starts once its file reaches the default branch. GitHub can
delay scheduled jobs; it is a daily supervision mechanism, not a real-time SLA.
See [GitHub's scheduling semantics](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

Even if verification fails, the dedicated job publishes current checked-branch
commits and up to50 open PRs in its job summary using read-only GitHub access.
That report gives agents fresh repository facts for repair; it does not certify
pipeline/service health or rewrite historical acceptance. No issues, messages,
Slack alerts or automated main-branch edits are created by the job.

For GitHub to reject merges mechanically, select the existing **verify** check
and/or **Handoff freshness** as required checks in main's branch protection/ruleset.
A local Git hook alone is bypassable; CI/ruleset policy is the merge enforcement
boundary. The delivered workflow fails on stale evidence; repository administrators
still control required checks and bypass permissions.

Automation proves freshness coverage and specific consistency conditions. It
cannot prove a prose claim is true, infer changed user intent, or guarantee current
external runtime availability. Agents must review those facts and record the
source/commit of evidence rather than auto-acknowledging them.
