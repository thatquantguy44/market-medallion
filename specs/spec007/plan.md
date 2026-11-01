# Implementation plan — NAAIM delayed public research feed

The user approved the separate delayed-public research capability
`naaim-exposure-public-delayed`. It is not the licensed NAAIM API feed and cannot
supply QuantMeridian.

## Dependency order

```text
written NAAIM permission + approved source-shaped sample
    -> schema update + invented fixtures
    -> parser/capture package
    -> local Bronze/Silver/research output
    -> explicit permitted live smoke run
```

## Decisions

- Use `naaim_exposure_public_delayed` as a source-owned package only after the
  provider documents the public-display structure and automation permission.
- Keep live acquisition disabled by default; tests use only invented fixtures
  and injected transport.
- Retain a three-month-delayed flag and exclude all QuantMeridian exports,
  schedules, bulk history and customer-facing redistribution.

## Risks and mitigations

| Risk | Mitigation |
| --- | --- |
| Public display changes or lacks structured values | Do not code a parser until an approved source-shaped sample exists; reject schema drift. |
| Automation/reuse is not allowed | Do not send a live request; close or retain the capability as documentation only. |
| Delayed data leaks into current product output | Use a source-specific dataset ID and prohibit QuantMeridian interfaces/schedules. |

## Approval checkpoint

Implementation is blocked at Task 0. The user approved the scope; NAAIM must
still supply external authorization and schema evidence before parser code can be
written safely.
