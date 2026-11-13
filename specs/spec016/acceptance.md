# Spec 016 local acceptance — 2026-11-13

The frozen study was rebuilt into a separate Git-ignored local MarketMedallion
workspace using the previously retained inputs. No provider request occurred.
The original donor research and archives were not modified. The workspace path
is an operator choice, not a runtime dependency of either repository.

## Source and review evidence

| Input | Retained SHA-256 | Original retrieval (UTC) |
| --- | --- | --- |
| Fama/French monthly factors ZIP | `b03d39e4bb253dda0f14893db3b78e64b40b8d6549a0fc0fd4111dae84c98618` | 2026-10-31 04:33:11 |
| FRED FEDFUNDS CSV | `74680b2d09bb28f90fb30c1c09e93ac2d443de1b7345ba5a9112b3eca9be0a25` | 2026-10-31 04:33:17 |
| Shiller workbook | `0df9392b7dacf91f756e92c8db508ad903c4588b43f3253680eec3dd8b40db68` | 2026-10-31 04:34:04 |

The Shiller capture used the historical HTTP exception; the retained hash does
not authenticate that original transport. The five official historical evidence
files, original fetch and control manifests, reviewed workbook and controls
are all content-pinned and preserved separately from Git. Their hashes and the
profile requirements are in `frozen.py`. Admission time is distinct from the
original recorded retrieval time. The controls carry the original review
attribution; this migration does not claim a new independent historical audit.

## Numerical and consumer parity

The donor code was rerun offline on selected copies of its normalized inputs.
Each of the five rebuilt datasets equals the producer dataset field for field:
88, 87, 87, 68 and 68 windows. All seven comparison, inherited and cycle
artifacts match byte for byte, including 86 paired cycle rows from 1937–2022.
The 1936 cycle omission and distinct year-end assignment remain explicit.
These checks establish reproduction of the donor study, not causal effects,
point-in-time vintages or exact S&P 500 replication.

The PRES browser acceptance loaded the actual frozen snapshot, checked the 87
common-sample, 68 inherited-sample and 86 cycle rows, and replayed an exported
run while the local pipeline routes were blocked. A backup was independently
restored into an absent local root with matching snapshot and catalog identities.
The fictional fixture has separate synthetic labels and separate counts.

## Operational and release boundaries

MarketMedallion, QuantMeridian and research-site handoff PRs have merged; the
release identities are recorded below. The selected historical snapshot retains
its source-tree digest and capture/review identities in the catalog. New live fetches remain blocked; the schedule definition is stopped by
default and no existing runtime schedule was altered. Recurring access, hosted
publication, source redistribution, S&P 500 licensing, and real-time historical
availability require separate decisions. See [source review](source_access_review.md).

The legacy donor `calendar_year_start` build option remains deliberately
unavailable in the strict PRES v1 publication path: it places a decision date
after that dataset's return-window start and violates the unchanged contract.
The frozen reconstruction explicitly uses `window_start`; independent manually
supplied presidential-period datasets remain supported under the existing schema.

## Recorded local verification

- Selected final local snapshot:
  `8815e2a9a2f2ea250dbb717ef411828a9c0d5a73a01a2a00c6b0f3d94fbde2c7`;
  catalog SHA-256 `0da65cf34978d77aebd396e5b4601cb18b9e1554b2ad3c196dd51beeafb94e90`.
- MarketMedallion: 1,245 passed / 92 skipped with socket-binding tests excluded
  under the filesystem sandbox; the nine local HTTP socket tests passed in a
  separate run with local socket access. Ruff check and format passed across the
  repository. The sdist and wheel built without isolation, and the installed wheel
  exposed the frozen-import CLI outside the source checkout. Skipped optional
  backends do not establish PostgreSQL or Delta qualification for this pipeline.
- QuantMeridian: 122 focused unit tests, typecheck and build passed; the actual
  historical snapshot passed the local browser and offline replay test.
- The original donor's 29 offline tests passed. A fresh donor rebuild exactly
  matched all five target datasets and all seven supporting artifacts.
- Local candidate heads at this checkpoint: MarketMedallion pipeline code
  `c76f1de` (operations and this handoff follow it), QuantMeridian `b653028`,
  research website `e66ae71`. These are feature-branch commits, not merged
  release SHAs; obtain final merge SHAs during the coordinated release.

## Coordinated release progress

- MarketMedallion [PR #21](https://github.com/thatquantguy44/market-medallion/pull/21)
  merged as `36e4f5c1162a76d1507a03b741b927b7652825ee` after both CI runs passed.
- QuantMeridian [PR #220](https://github.com/thatquantguy44/QuantMeridian/pull/220)
  merged as `ad986a84dda4a301d6d2ae4cbb3a917b5c0fe3a6` after GitHub CI passed.
- [Research-site PR #22](https://github.com/thatquantguy44/thatquantguy44.github.io/pull/22)
  merged as `3e051f98e099843fb11d8f6871e9517ba08b9920`.
- QuantMeridian documentation [PR #221](https://github.com/thatquantguy44/QuantMeridian/pull/221)
  merged as `ece4d99f2fb9b22c093c4419a9af9952551ab595` after GitHub CI passed.
- The merged MarketMedallion wheel was built and installed outside its source
  checkout; the installed CLI verified the selected five-dataset snapshot.
  Producer and consumer contract files were byte-identical.

Vercel preview provisioning failed before a build started, including on a retry;
prior preview deployments reported the same error before this branch existed.
The production deployment for QuantMeridian merge `ad986a84` reached READY.
The separate QuantMeridian-Core sync workflow still fails its pre-existing patch,
secret-scan and subset checks; no downstream synchronization is claimed.

The `presidential_research_refresh` schedule is defined stopped by default and
was not started by this delivery. On 2026-11-13, the local Dagster webserver was
reachable but its code location reported `DagsterUserCodeUnreachableError`, so
its effective runtime schedule state could not be independently verified. A
read-only query of the local instance schedule database found no persisted row
for `presidential_research_refresh`. Keep live acquisition blocked and
investigate the local code server before relying on its operational status.
