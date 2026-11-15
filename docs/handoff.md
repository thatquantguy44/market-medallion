# Project handoff entrypoint

Start at the [master handoff's current panel](handoffs/project_bootstrap.md#agent-start-here)
for priorities, active/completed work, commit/PR references and the exact next task.
The [freshness guard](development/handoff_freshness.md) checks it against source,
canonical task state and its review evidence in CI and daily.

This entrypoint deliberately links the current record instead of maintaining a
second competing priority queue. The root [AGENTS.md](../AGENTS.md) describes how
agents must update the master panel before handing off.

- [Pipeline register](pipelines.md): implemented/planned packages and ownership.
- [Dagster register](deployment/dagster_jobs.md): job/schedule controls.
- [Spec index](../specs/README.md): contracts and acceptance scope.
- [Agent libraries](../agents/README.md): pinned development workflows.

NAAIM remains paused until the user explicitly resumes it. Historical checkpoints
remain in the master handoff; old source tasks are not the current priority queue.
