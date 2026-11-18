# ICI weekly flows — T01 source and use review

Reviewed: 2026-11-18 UTC. **Decision: fixture-only; real-data admission blocked.**
This is a documentary review, not an ICI license determination or a source-data
capture. No user-specific agreement, subscription, permission letter or authorized
source file has been provided or verified. No ICI observation file was requested.

## Official evidence

| Source | What the page establishes | What it does not establish |
| --- | --- | --- |
| [ICI weekly ETF net issuance](https://www.ici.org/research/statistics/etfs/weekly-estimated-etf-net-issuance) | Lists a weekly release and an XLS summary; links to ICI's permission request. | Download automation rights, XLS reuse, historical coverage or a stable file URL. |
| [ICI weekly long-term mutual fund flows](https://www.ici.org/research/statistics/mutual-funds/weekly-estimated-longterm-mutual-fund-flows) | Lists a weekly release and an XLS summary; links to ICI's permission request. | Download automation rights, XLS reuse, historical coverage or a stable file URL. |
| [ICI permissions](https://www.ici.org/permissions) | Says underlying statistical-publication data are available to ICI members and subscribers only and are not to be republished; permission requests are reviewed by ICI. | Whether this project is a member/subscriber, or whether a particular operation is permitted by its agreement. |
| [ICI copyright and linking policy](https://www.ici.org/copyright-and-linking-policies) | Permits personal computer storage for personal use and attributed citation/reference; restricts distribution, reproduction, modification, reuse and mirroring without permission. | General permission for a shared warehouse, automated capture, transformed artifact or internal distribution. |

The public release text and the linked statistical XLS may have different access
conditions. Seeing a release on a public page does not admit a data pipeline.
The two populations need separate artifact and use evidence. This review does
not infer rights from the existing MarketMedallion FINRA or other providers.

## Route and operation decision

| Route | Data status | Admission basis | Local import | Automated acquire | Retain / transform | Internal export |
| --- | --- | --- | --- | --- | --- | --- |
| Invented fixture | Synthetic only | None; explicit fixture label | No | No | No ICI data | No |
| Operator-supplied ICI file | **Blocked** until source owner records ICI permission or subscriber agreement for the named series and each operation | Nonsecret evidence ID plus approved audience and terms review | Pending | No, even after a local-file grant | Pending explicit grants | Pending explicit grant |
| ICI download | **Blocked** | Separate admitted route, exact artifact and explicit automated-access grant | Pending | Pending explicit grant | Pending explicit grants | Pending explicit grant |

The pure `require_approved_use` validator accepts only enumerated operations
for one of the two series, fails closed on absent/expanded profiles, and keeps
the permission reference to a short nonsecret identifier. A passing validator
check is an **operator assertion**, not independent evidence that ICI granted
rights. No active real-data profile ships in Git. Future acquisition, storage,
transform and export adapters must enforce their own relevant operation before
I/O. Credentials and agreement text belong outside this repository.

## Missing approval record

| Item | Current state | Required to unblock |
| --- | --- | --- |
| Approving source/data owner | Unassigned | Name the person or team authorized to review this project's ICI rights. |
| Agreement / permission reference | Not supplied | Record a nonsecret evidence ID and where the agreement is held privately. |
| Covered series | Not established | Identify ETF, mutual fund, or both explicitly. |
| Local storage and retention | Not established | Verify warehouse/raw retention and backup scope, not just personal storage. |
| Automated access | Not established | Record exact permitted route, frequency, authentication and rate limits before a downloader exists. |
| Transform and internal sharing | Not established | Record approved derivative use, audience, attribution and export restrictions. |

## Next gate

The source owner must review an applicable ICI agreement or obtain written
permission and attest the exact allowed operations for each series. T02 then
reviews authorized, retained-out-of-Git source-shaped artifacts. Until then,
only invented fixtures and pure admission code are in scope. No source shape,
permission, publication clock or historical coverage is claimed here.
