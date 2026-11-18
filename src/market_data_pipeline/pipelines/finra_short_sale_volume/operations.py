"""SSV-AC13: offline release-aware health and conservative retention reports."""

from datetime import UTC, datetime, timedelta

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest
from market_data_pipeline.pipelines.finra_short_sale_volume.data.release import (
    day,
    default_policy,
    load_policy,
    release_status,
    validate_policy,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.io import bronze
from market_data_pipeline.pipelines.finra_short_sale_volume.io.operations_snapshot import (
    read_snapshot,
)
from market_data_pipeline.pipelines.finra_short_sale_volume.registry import select_scopes

__all__ = ["health", "retention_dry_run", "default_policy", "load_policy"]


def health(
    root,
    manifest,
    *,
    start,
    end,
    scopes,
    approved_use,
    operation,
    policy=None,
    clock=lambda: datetime.now(UTC),
    **budgets,
):
    policy = validate_policy(default_policy() if policy is None else policy)
    now = clock()
    observed_at = bronze._clock(now)
    first, last = day(start), day(end)
    if not 0 <= (last - first).days < 366:
        raise ValueError("FINRA health date range must contain 1..366 days")
    facilities = select_scopes(scopes)
    if not {f.id for f in facilities} <= {f.id for f in manifest.facilities}:
        raise ValueError("FINRA health scopes exceed manifest")
    with read_snapshot(
        root, manifest, approved_use=approved_use, operation=operation, **budgets
    ) as saved:
        rows, degraded, unknown = [], False, False
        state, results, receipts = saved["state"], saved["results"], saved["receipts"]
        unresolved = sorted(set(receipts) - set(results))
        clocks = [r["archived_at"] for r in receipts.values()]
        clocks.extend(r["completed_at"] or r["started_at"] for r in saved["runs"])
        clocks.extend(r["completed_at"] or r["started_at"] for r in state["attempts"])
        if any(value > observed_at for value in clocks):
            raise ValueError("FINRA health clock precedes retained evidence")
        for offset in range((last - first).days + 1):
            selected = (first + timedelta(days=offset)).isoformat()
            for facility in facilities:

                def matches(row, selected=selected, facility_id=facility.id):
                    return (row["trade_date"], row["facility_id"]) == (selected, facility_id)

                partition = next((r for r in state["partitions"] if matches(r)), None)
                committed = results[partition["capture_id"]] if partition else None
                count = committed["accepted_rows"] if committed else 0
                attempts = [r for r in state["attempts"] if matches(r)]
                last_attempt = attempts[-1] if attempts else None
                observations = [
                    (r, p) for r in saved["runs"] for p in r["partitions"] if matches(p)
                ]
                successful = [
                    r["completed_at"] for r, p in observations if p["status"] == "APPLIED"
                ]
                latest = observations[-1] if observations else None
                refresh, failure = ("LOCAL_ONLY" if committed else "MISSING"), None
                if latest:
                    r, p = latest
                    ties = sum(other["started_at"] == r["started_at"] for other, _ in observations)
                    refresh = (
                        "AMBIGUOUS"
                        if ties > 1
                        else (
                            "SUCCESS"
                            if p["status"] == "APPLIED"
                            else ("RUNNING" if r["status"] == "RUNNING" else "FAILED")
                        )
                    )
                    if refresh == "FAILED":
                        failure = dict(
                            run_id=r["run_id"], status=p["status"], completed_at=r["completed_at"]
                        )
                local_success = committed["completed_at"] if committed else None
                if local_success is not None and local_success > observed_at:
                    raise ValueError("FINRA health clock precedes retained evidence")
                capture_at = (
                    (
                        receipts[partition["capture_id"]]["retrieved_at"]
                        or receipts[partition["capture_id"]]["archived_at"]
                    )
                    if partition
                    else None
                )
                stale = bool(
                    capture_at
                    and (now - datetime.fromisoformat(capture_at)).total_seconds()
                    > policy["max_capture_age_hours"] * 3600
                )
                release = release_status(selected, now, policy)
                pre_release = bool(
                    capture_at
                    and release["expected_at"]
                    and datetime.fromisoformat(capture_at)
                    < datetime.fromisoformat(release["expected_at"])
                )
                if committed and release["status"] == "OVERDUE" and not pre_release:
                    release["status"] = "OBSERVED"
                coverage = ("COMPLETE" if count else "VALID_EMPTY") if committed else "MISSING"
                local_failure = last_attempt is not None and last_attempt["status"] != "APPLIED"
                pending = release["status"] == "PENDING"
                missing_due = release["status"] == "OVERDUE"
                degraded |= (
                    stale
                    or local_failure
                    or refresh in {"FAILED", "RUNNING", "AMBIGUOUS"}
                    or missing_due
                )
                unknown |= release["status"].startswith("UNKNOWN")
                rows.append(
                    dict(
                        trade_date=selected,
                        facility_id=facility.id,
                        coverage=coverage,
                        committed_capture_id=partition["capture_id"] if partition else None,
                        committed_rows=count,
                        latest_local_attempt_status=last_attempt["status"]
                        if last_attempt
                        else None,
                        last_local_success_at=local_success,
                        last_acquisition_success_at=max(successful) if successful else None,
                        refresh_status=refresh,
                        trailing_failure=failure,
                        stale_capture=stale,
                        last_capture_at=capture_at,
                        pre_release_capture=pre_release,
                        release_status=release["status"],
                        expected_release_at=release["expected_at"],
                        pending_release=pending,
                    )
                )
        pub = saved["publication"]
        if pub["current_generation_id"] and any(
            r["capture_id"] not in pub["current_partition_capture_ids"] for r in state["partitions"]
        ):
            pub["status"] = (
                "SOURCE_ADVANCED" if not pub["pending_generation_ids"] else "RECOVERY_REQUIRED"
            )
        degraded |= bool(unresolved) or pub["status"] in {"SOURCE_ADVANCED", "RECOVERY_REQUIRED"}
        return dict(
            schema_id="finra-ssv-health/1",
            status="DEGRADED"
            if degraded
            else (
                "UNKNOWN"
                if unknown
                else ("PENDING" if any(r["pending_release"] for r in rows) else "HEALTHY")
            ),
            observed_at=observed_at,
            policy=policy,
            requested_partitions=len(rows),
            observed_partitions=sum(r["committed_capture_id"] is not None for r in rows),
            pending_releases=sum(r["pending_release"] for r in rows),
            unresolved_capture_ids=unresolved,
            publication=pub,
            partitions=rows,
            evidence_sha256=digest(saved["files"]),
            knowledge_basis="CURRENT_VINTAGE",
            clock_basis="LOCAL_CAPTURE",
        )


def retention_dry_run(root, manifest, *, approved_use, operation, policy=None, **budgets):
    policy = validate_policy(default_policy() if policy is None else policy)
    with read_snapshot(
        root, manifest, approved_use=approved_use, operation=operation, **budgets
    ) as saved:
        reasons = {capture_id: {"PRESERVE_ALL_POLICY"} for capture_id in saved["receipts"]}
        for row in saved["state"]["partitions"]:
            reasons[row["capture_id"]].add("CURRENT_PARTITION")
        for row in saved["state"]["versions"]:
            reasons[row["capture_id"]].add("VERSION_ORIGIN")
        for row in saved["state"]["capture_rows"]:
            reasons[row["capture_id"]].add("OBSERVATION_MEMBERSHIP")
        for row in saved["state"]["attempts"]:
            reasons[row["capture_id"]].add("ATTEMPT_AUDIT")
        for run in saved["runs"]:
            for capture_id in run["capture_ids"]:
                reasons[capture_id].add("ACQUISITION_JOURNAL")
        for capture_id in saved["published_captures"]:
            reasons[capture_id].add("FROZEN_PUBLICATION")
        parents = {}
        for parent, children in saved["collections"].items():
            for child in children:
                reasons[child].add("COLLECTION_INPUT")
                parents.setdefault(child, []).append(parent)
        for capture_id in set(reasons) - set(saved["results"]):
            reasons[capture_id].add("UNRESOLVED_CAPTURE")
        captures = [
            dict(
                capture_id=capture_id,
                schema_id=receipt["schema_id"],
                payload_bytes=receipt["byte_count"],
                reasons=sorted(reasons[capture_id]),
                collection_parents=sorted(parents.get(capture_id, [])),
                publication_generations=sorted(saved["published_captures"].get(capture_id, [])),
            )
            for capture_id, receipt in sorted(saved["receipts"].items())
        ]
        membership = {}
        for row in saved["state"]["capture_rows"]:
            membership.setdefault(row["version_id"], set()).add(row["capture_id"])
        files = []
        for row in saved["files"]:
            parts = row["path"].split("/")
            held = {"PRESERVE_ALL_POLICY"}
            if (
                len(parts) >= 3
                and parts[0] == "bronze"
                and parts[1] in {"captures", "outcomes"}
                and parts[2] in reasons
            ):
                held |= reasons[parts[2]]
                held.add("RAW_OR_PARSE_EVIDENCE")
            elif parts[:2] == ["bronze", "sequences"] or row["path"] == "bronze/format.json":
                held.add("ARCHIVE_IDENTITY_AND_ALLOCATOR")
            elif parts[0] == "warehouse":
                held.add("WAREHOUSE_STATE_AND_LINEAGE")
            elif parts[0] == "acquisition":
                held.add("ACQUISITION_JOURNAL")
            elif parts[:2] == ["parquet", ".staging"]:
                held.add("PUBLICATION_STAGE_HOLD")
            elif parts[:2] == ["parquet", "snapshots"]:
                held.add("FROZEN_PUBLICATION")
            elif parts[0] == "parquet":
                held.add("PUBLICATION_CATALOG_AND_POINTER")
            elif row["path"] in {
                "warehouse.json",
                "acquisition.json",
                "parquet.json",
                ".postgres-primary.json",
            }:
                held.add("STORE_IDENTITY")
            elif row["path"] == "replay.json":
                held.add("REPLAY_PROVENANCE")
            elif row["path"] in {".acquire.lock", ".writer.lock", ".ingest.lock"}:
                held.add("COORDINATION")
            else:
                held.add("UNCLASSIFIED_HOLD")
            files.append(dict(row, action="KEEP", reasons=sorted(held)))
        return dict(
            schema_id="finra-ssv-retention-dry-run/1",
            dry_run=True,
            policy=policy["retention"],
            deletion_enabled=False,
            deletable_bytes=0,
            total_bytes=sum(r["bytes"] for r in files),
            version_count=len(saved["state"]["versions"]),
            capture_count=len(captures),
            files=files,
            captures=captures,
            publication=saved["publication"],
            versions=[
                dict(
                    version_id=r["version_id"],
                    capture_id=r["capture_id"],
                    membership_capture_ids=sorted(membership.get(r["version_id"], [])),
                    publication_generations=sorted(
                        saved["published_versions"].get(r["version_id"], [])
                    ),
                )
                for r in saved["state"]["versions"]
            ],
            evidence_sha256=digest(saved["files"]),
        )
