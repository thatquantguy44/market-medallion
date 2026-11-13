# SSV-T10 — typed, coherent Parquet publication

Selected 2026-11-13 on `feat/finra-parquet-publication`, from merged main `55dcd5d`
(PR #23). Traces SSV-REQ10 / SSV-AC10 and common C04–C07. Implement in tested
increments: exact typed schemas/readback; frozen source projection and publication
catalog; staged publication/recovery; CLI and acceptance. T11 replay and later
acquisition, source-aware health, exchange and jobs remain separate.

## Typed projection and precision

Publish an explicit trade date, disjoint facility selection, local cutoff and
bounded lookback using the T09 request semantics. Require complete current
partitions. Retain Silver versions/removals, current per-facility Gold, scoped
research Gold, coverage, terminal captures, capture membership and run attempts
within the selected date window and cutoff. Preserve current/prior component
lineage, original clocks/sequences and all distinct absence/error states. The
source is already captured local data, not a history acquisition request.

Fixed schemas use date32 for source dates, UTC microsecond timestamps for clocks,
int64 for counters/sequences, booleans for flags and strings for identifiers.
Decimal128/256 cannot hold the supported 4096-digit source values or wider T09
exact sums. Use an explicitly versioned exact-decimal struct: negative:boolean,
digits:string and exponent:int32. Decode to the original canonical Decimal text,
retaining signed zero and scale. No float, coefficient truncation or quantization.
Schema metadata names the codec/unit; schema fingerprints bind each table.
Critical fields are typed columns; canonical `record_json` additionally preserves
the full normalized document. Readback must reconstruct every original document
and agree with every typed projection, schema, row count and logical checksum.
Empty tables carry the same exact schema. Unsupported metadata integer widths,
invalid decimal representations, unknown tables and budget excess refuse.

## Database/file protocol

The operational SQLite warehouse remains the ingestion/inspection store. A
separate source-owned publication catalog stores immutable generation rows and
the publication journal. This avoids changing the accepted ingestion schema and
lets a published generation remain coherent while ingestion advances. Published
readers use the catalog and files selected by `parquet/current.json`, not mutable
operational Gold. A successful ingestion audit is not a file-publication watermark.

All publication/recovery operations share the root ingestion lock. Capture one
verified source database snapshot and research projection, under that lock and
a pinned SQLite read transaction. Then:

1. Stage private typed files, read them back, hash and fsync files/directories.
2. In one catalog transaction, insert immutable snapshot facts plus COMMITTED
   journal metadata. This is the publication database commit boundary.
3. Rename/fsync the staged directory into its immutable final generation and
   verify its manifest/file/schema/logical identities against the catalog.
4. Atomically replace/fsync the sole current pointer.
5. Mark the journal PUBLISHED. Only this advances the publication success record.

A failure before the catalog commit cannot advance the current pointer. A
COMMITTED generation must recover from its exact staged/final bytes before a
new publish is allowed. Recovery verifies a pointer already advanced before the
final journal mark. A reader may use that fully verified new generation with
`recovery_required` while its final mark is pending; it never claims a false
PUBLISHED watermark. Otherwise the previous coherent generation stays selected.
Corrupt/missing committed artifacts or identity mismatches block recovery/new
publication rather than inventing replacement evidence. Verified owned staging
orphans without a committed journal may be discarded. Final files are never globbed
or replaced. Snapshot UUID retries must match request/data; unchanged current
input may reuse its generation. A later return to an older projection creates a
new publication event, not a pointer rollback.

The catalog, root binding, source-warehouse identity, manifest, input kind and
schema contract must agree. Missing/foreign bound stores, symlinks, path traversal,
corrupt pointers, unexpected stage contents and conflicting generation IDs refuse.
This is local POSIX publication; no remote backup, service or source activation.

## Bounds, commands and verification

Default total rows250000 and logical/Arrow/file bytes128MiB are explicit finite
publication/read limits; normalized documents have a262144-byte bound. Validate
counts and estimated source bytes before loading. Validate actual encoded/readback
sizes, schemas and counts too. Limits are operating budgets, not provider widths.
Publication history is bounded; audit/configuration contains no credentials,
absolute filesystem paths, raw response bodies or arbitrary exception text.

Expose local `publish`, `published` and `recover-publication` through the existing
script/config. Publication is explicit; existing run/gold/research commands retain
their accepted behavior. Fixture/provider admission is checked before I/O.

Invented tests must cover whole/fractional/4096-digit/exponent values, signed zero,
null/removal/empty rows, full typed schema round trips, conflicting/corrupt files,
source/cutoff lineage, row/byte budgets, retries/concurrent calls and no network.
Inject failure at staging, catalog commit, finalization, pointer publication and
journal completion. Restart must select the prior coherent generation or finish
the exact new one. Verify frozen catalog facts equal decoded Parquet at each
boundary. Record actual CLI conformance, raw/operational-store preservation,
all FINRA/full offline tests, lint/format and package/source readback.

References reviewed before coding: [Arrow structs](https://arrow.apache.org/docs/python/generated/pyarrow.struct.html),
[Arrow decimal256 precision](https://arrow.apache.org/docs/python/generated/pyarrow.decimal256.html),
[Parquet](https://arrow.apache.org/docs/python/parquet.html) and
[FINRA API contract](https://developer.finra.org/docs). Runtime remains pinned to
PyArrow23.0.1; no provider schema or use-grant change is inferred.
