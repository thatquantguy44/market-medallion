# SEC FTD offline fixtures

All identifiers, symbols, descriptions and amounts in `modern.txt` are invented.
The six header labels follow the SEC's documented field table. The two terminal
count/quantity labels match the approved local September 2026 archives reconciled
on 2026-11-03; all fixture totals are invented. See
[the source checkpoint](../../../specs/spec011/source_review.md) for bounded
sample evidence, including the separate unsuccessful HTTP request.

Tests construct ZIP containers in memory from this text, including unsafe,
corrupt, duplicated and oversized variants. No real provider archive is tracked.
`000000001` demonstrates string identifier preservation; the large count exceeds
64-bit integer precision; `.` is the SEC-documented missing-price token.
