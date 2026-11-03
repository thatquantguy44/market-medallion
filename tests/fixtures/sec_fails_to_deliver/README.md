# SEC FTD offline fixtures

All identifiers, symbols, descriptions and amounts in `modern.txt` are invented.
The six header labels follow the SEC's documented field table. A real modern
archive has not yet been characterized: the one bounded source request on
2026-11-03 returned HTTP403. These fixtures prove the documentary profile only.

Tests construct ZIP containers in memory from this text, including unsafe,
corrupt, duplicated and oversized variants. No real provider archive is tracked.
`000000001` demonstrates string identifier preservation; the large count exceeds
64-bit integer precision; `.` is the SEC-documented missing-price token.
