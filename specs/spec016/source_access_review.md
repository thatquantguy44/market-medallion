# PR016 source access and transport review — 2026-11-13

No provider data was downloaded for this review. The earlier project closed
additional acquisition and approved reprocessing its existing archives; a new
refresh is a separate source-specific decision. All live sources remain blocked.
No API key is read and no provider is probed by a blocked run.

- Fama/French: the [official library](https://mba.tuck.dartmouth.edu/pages/Faculty/ken.french/data_library.html)
  publishes monthly factor files and identifies the January 2025 change from FIZ
  to CIZ returns. Recurring retrieval and intended distribution remain to be
  reviewed independently; public availability alone is not a license decision.
- Shiller: the historical catalog's URL is HTTP, which cannot be admitted as a
  verified TLS connection. A current official HTTPS endpoint and transport
  policy require verification; no third-party mirror is substituted.
- FRED: [current terms](https://fred.stlouisfed.org/legal/) include restrictions
  on development of software and AI, while the [API terms](https://fred.stlouisfed.org/docs/api/terms_of_use.html)
  contain separate use and attribution requirements. Their application to this
  development and archived data workflow needs clarification from the provider
  or an authorized reviewer. No conclusion is made about the user's other uses.
- House and Wikipedia: candidates and cross-checks are not approved political
  controls. Recurring retrieval, attribution and retention still need review.
- Manual official control evidence is reviewed separately and is not fetched by
  automated source loops.

The frozen historical inputs and previously dated controls are not newly
retrieved. Their exact bytes, capture provenance and applicable use rights must
be checked before the real-data acceptance is recorded. Fictional test
transports are always classified as synthetic and cannot enable a live source.
