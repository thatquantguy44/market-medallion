"""SSV-AC13: read-only, bounded acquisition evidence; never initialize or resume."""

import re

from market_data_pipeline.pipelines.finra_short_sale_volume.data.acquisition import Limits, plan
from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest
from market_data_pipeline.pipelines.finra_short_sale_volume.data.release import day
from market_data_pipeline.pipelines.finra_short_sale_volume.io import acquisition_journal, bronze
from market_data_pipeline.pipelines.finra_short_sale_volume.io.replay_archive import _paths

STATUSES = {
    "NOT_ATTEMPTED",
    "APPLIED",
    "PARTIAL",
    "REJECTED",
    "TRANSPORT_FAILED",
    "UNSAFE_PAYLOAD",
    "EXECUTION_FAILED",
    "ARCHIVED",
}


def _read(root, manifest, kind, *, max_runs, max_events):
    directory = root / "acquisition"
    binding = root / "acquisition.json"
    if not directory.exists() and not binding.exists():
        return []
    expected = dict(
        schema_id=acquisition_journal.CONTRACT,
        root_sha256=digest(str(root.resolve())),
        manifest_sha256=digest(manifest.to_dict()),
        input_kind=kind,
    )
    identity = bronze._json(binding)
    bronze._identity(identity.get("journal_id"))
    if (
        set(identity) != set(expected) | {"journal_id"}
        or any(identity[k] != v for k, v in expected.items())
        or bronze._json(directory / "identity.json") != identity
        or directory.is_symlink()
        or (directory / "runs").is_symlink()
    ):
        raise ValueError("FINRA acquisition health binding mismatch")
    runs, event_count = [], 0
    for path in _paths(directory / "runs", max_runs):
        run_id = bronze._identity(path.name)
        request, result = bronze._json(path / "request.json"), bronze._json(path / "result.json")
        if (
            set(request) != {"schema_id", "run_id", "plan", "admission_sha256", "started_at"}
            or request["schema_id"] != acquisition_journal.CONTRACT
            or request["run_id"] != run_id
            or result.get("run_id") != run_id
            or not re.fullmatch(r"[0-9a-f]{64}", request["admission_sha256"])
        ):
            raise ValueError("FINRA acquisition health request mismatch")
        started = bronze._clock_text(request["started_at"])
        saved = request["plan"]
        scopes = list(dict.fromkeys(p["facility_id"] for p in saved["partitions"]))
        expected_plan = plan(
            manifest,
            saved["requested_start"],
            saved["requested_end"],
            scopes,
            as_of=day(saved["as_of"]),
            overlap_days=saved["overlap_days"],
            limits=Limits(**saved["limits"]),
        )
        if saved != expected_plan:
            raise ValueError("FINRA acquisition health plan mismatch")
        events, references = [], set()
        for event_path in sorted(_paths(path / "events", max_events)):
            event_count += 1
            if event_count > max_events:
                raise ValueError("FINRA health total event budget exceeded")
            event = bronze._json(event_path)
            sequence = len(events) + 1
            if (
                event_path.name != f"{sequence:020d}.json"
                or event.get("event_sequence") != sequence
                or event.get("run_id") != run_id
                or bronze._clock_text(event.get("recorded_at")) < started
                or (events and event["recorded_at"] < events[-1]["recorded_at"])
            ):
                raise ValueError("FINRA acquisition health event mismatch")
            if event.get("capture_id") is not None:
                references.add(bronze._identity(event["capture_id"]))
            events.append(event)
        running = result == dict(
            schema_id=acquisition_journal.CONTRACT, run_id=run_id, status="RUNNING"
        )
        if running:
            partitions = [dict(p, status="RUNNING", capture_id=None) for p in saved["partitions"]]
            completed = None
        else:
            completed = bronze._clock_text(result.get("completed_at"))
            partitions = result.get("partitions")
            if (
                result.get("schema_id") != "finra-ssv-acquisition-result/1"
                or result.get("status") not in {"COMPLETE", "INCOMPLETE"}
                or result.get("plan") != saved
                or result.get("events") != len(events)
                or completed < started
                or (events and completed < events[-1]["recorded_at"])
                or not isinstance(partitions, list)
                or len(partitions) != len(saved["partitions"])
                or result.get("planned_partitions") != len(partitions)
            ):
                raise ValueError("FINRA acquisition health result mismatch")
            for partition, selected in zip(partitions, saved["partitions"], strict=True):
                if (
                    any(partition.get(k) != v for k, v in selected.items())
                    or partition.get("status") not in STATUSES
                ):
                    raise ValueError("FINRA acquisition health partition mismatch")
                if partition.get("capture_id") is not None:
                    references.add(bronze._identity(partition["capture_id"]))
            complete = sum(p["status"] == "APPLIED" for p in partitions)
            if result.get("complete_partitions") != complete or (
                result["status"] == "COMPLETE"
                and (complete != len(partitions) or result.get("error_code") is not None)
            ):
                raise ValueError("FINRA acquisition health success mismatch")
            metadata = result.get("metadata_sha256")
            if metadata is not None:
                import hashlib

                if (
                    hashlib.sha256(bronze._read(path / "metadata.json", 65536)).hexdigest()
                    != metadata
                ):
                    raise ValueError("FINRA acquisition metadata hash mismatch")
        runs.append(
            dict(
                run_id=run_id,
                started_at=started,
                completed_at=completed,
                status=result["status"],
                partitions=partitions,
                capture_ids=sorted(references),
            )
        )
    return sorted(runs, key=lambda r: (r["started_at"], r["run_id"]))


def read(root, manifest, kind, *, max_runs, max_events):
    try:
        return _read(root, manifest, kind, max_runs=max_runs, max_events=max_events)
    except (KeyError, TypeError, AttributeError, UnicodeError, RecursionError):
        raise ValueError("FINRA acquisition health journal is malformed") from None
