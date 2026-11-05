# NAAIM delayed public research tasks

## Task 0: Obtain provider authorization and source contract

**Acceptance criteria:**

- [ ] Written NAAIM approval identifies automation frequency, retention,
  internal-use scope and permitted transformations.
- [ ] A permitted, redacted source-shaped sample documents value/date/units and
  expected failure behavior without credentials or a hidden endpoint.

**Verify:** Attach the approved evidence to Spec007 and update its source
schema/access sections.

**Dependencies:** None. This blocks recurring acquisition, persistence
expansion and downstream implementation.

**Current evidence:** On 2026-11-01 the user reported oral NAAIM permission by
phone. The user then confirmed daily automated pulls and 365-day retention. The
corresponding nonsecret receipt reference is
`user-confirmed-naaim-permission-2026-11-01`. The user subsequently confirmed
daily automated pulls, 365-day retention and the local research-report display
scope. The enabled local schedule and separately gated local export remain
limited to that delayed research boundary.

## Task 1: Define fixture and parser contract

**Acceptance criteria:**

- [x] Invented fixtures cover a valid delayed observation, absent fields,
  changed observation, malformed numbers/dates and an empty table.
- [x] Unit tests prove source/date/unit preservation, schema rejection,
  duplicate rejection and HTTP 429/5xx rejection.

**Verify:** Focused pytest module for `naaim_exposure_public_delayed`.

**Dependencies:** Task 0.

**Progress:** The verified table headers, invented fixture suite and strict
parser are implemented. No live provider values are committed to fixtures.

## Task 2: Implement the local research-only vertical slice

**Acceptance criteria:**

- [x] Source-owned Bronze receipts, typed Silver versions and attributed delayed
  research output work from fixtures.
- [x] Live retrieval requires a nonsecret permission reference.
- [x] The source package has no credential-sharing, database or networked
  QuantMeridian path; the explicit local artifact export is attributed,
  delayed and current-vintage only.

**Verify:** Focused tests, full suite, lint, format and package build.

**Dependencies:** Task 1.

**Progress:** Local immutable Bronze capture, append-only observed versions,
an attributed delayed latest output and a narrow current-vintage research export
are implemented. The manual module CLI passed one bounded smoke run on
2026-11-01 (131 rows); no bulk backfill, database adapter or networked
QuantMeridian access exists.

## Task 3: Add local-only operational visibility

**Acceptance criteria:**

- [x] A read-only health check reports local capture freshness, retained-version
  count and attributed delayed-output availability without a provider request.
- [x] Delayed observation age is not treated as source freshness.
- [x] The manual CLI returns zero only for a healthy local state and cannot use
  `--check` to make a live request.

**Verify:** Focused NAAIM tests and the local `--check` command against the
ignored smoke root.

**Dependencies:** Task 2.

**Progress:** The check passed against the 2026-11-01 smoke capture. Bulk
history, database persistence and QuantMeridian output remain out of scope.

## Task 4: Add permission-gated daily operations

**Acceptance criteria:**

- [x] A stopped daily 10:00 America/New_York Dagster schedule requires explicit
  runtime acknowledgement, pipeline root and nonsecret permission reference.
- [x] Each successful scheduled capture records a distinct capture event even
  when payload bytes are unchanged.
- [x] Source-owned 365-day retention is dry-run-first and rebuilds retained
  Silver/Gold state from active capture snapshots before deleting expired data.

**Verify:** Focused NAAIM/Dagster tests; no schedule daemon or live daily run is
enabled by this implementation.
