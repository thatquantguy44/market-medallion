"""SSV-AC12: root-bound acquisition runs, immutable request events and atomic results."""

import fcntl
import os
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

from market_data_pipeline.pipelines.finra_short_sale_volume.data.capture import digest, encoded
from market_data_pipeline.pipelines.finra_short_sale_volume.io.bronze import (
    _admit,
    _clock,
    _identity,
    _json,
    _publish_directory,
    _put,
    _sync,
    _write,
)

CONTRACT = "finra-ssv-acquisition-journal/1"


def replace(path, value):
    body = encoded(value)
    if len(body) > 65536:
        raise ValueError("FINRA acquisition journal document exceeds its bound")
    temporary = path.parent / (".write-" + uuid4().hex + ".tmp")
    try:
        _write(temporary, body)
        os.replace(temporary, path)
        _sync(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


class Run:
    def __init__(self, path, run_id, clock):
        self.path, self.run_id, self.clock = path, run_id, clock
        self.events = 0

    def event(self, value):
        if self.events >= 5000:
            raise ValueError("FINRA acquisition event budget exceeded")
        value = dict(
            value,
            run_id=self.run_id,
            event_sequence=self.events + 1,
            recorded_at=_clock(self.clock()),
        )
        if len(encoded(value)) > 65536:
            raise ValueError("FINRA acquisition event exceeds metadata bound")
        _put(self.path / "events" / f"{self.events + 1:020d}.json", encoded(value))
        self.events += 1

    def finish(self, value):
        result = dict(
            value, run_id=self.run_id, completed_at=_clock(self.clock()), events=self.events
        )
        replace(self.path / "result.json", result)
        return result

    def metadata(self, body):
        _put(self.path / "metadata.json", body)


@contextmanager
def open_run(config, plan, *, clock):
    config.validate()
    kind = _admit(config.manifest, config.approved_use, config.operation, {"fixture", "acquire"})
    root = Path(config.root)
    if root.is_symlink():
        raise ValueError("FINRA acquisition root is unsafe")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(root / ".acquire.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        directory = root / "acquisition"
        binding = root / "acquisition.json"
        expected = dict(
            schema_id=CONTRACT,
            root_sha256=digest(str(root.resolve())),
            manifest_sha256=digest(config.manifest.to_dict()),
            input_kind=kind,
        )
        if not directory.exists() and not directory.is_symlink():
            if binding.exists() or binding.is_symlink():
                raise ValueError("FINRA bound acquisition journal is missing")
            identity = dict(expected, journal_id=str(uuid4()))
            _publish_directory(
                root, "acquisition", {"identity.json": encoded(identity), "runs": None}
            )
            _put(binding, encoded(identity))
        if (
            directory.is_symlink()
            or (directory / "runs").is_symlink()
            or not (directory / "runs").is_dir()
        ):
            raise ValueError("FINRA acquisition journal directory is missing or unsafe")
        identity = _json(directory / "identity.json")
        _identity(identity.get("journal_id"))
        if (
            set(identity) != set(expected) | {"journal_id"}
            or _json(binding) != identity
            or any(identity[k] != v for k, v in expected.items())
        ):
            raise ValueError("FINRA acquisition journal binding mismatch")
        count = 0
        for path in (directory / "runs").iterdir():
            _identity(path.name)
            if path.is_symlink() or not path.is_dir():
                raise ValueError("FINRA acquisition run inventory is unsafe")
            count += 1
            if count >= 10000:
                raise ValueError("FINRA acquisition run inventory budget exceeded")
        run_id = str(uuid4())
        request = dict(
            schema_id=CONTRACT,
            run_id=run_id,
            plan=plan,
            admission_sha256=digest(config.approved_use),
            started_at=_clock(clock()),
        )
        if len(encoded(request)) > 65536:
            raise ValueError("FINRA acquisition request metadata budget exceeded")
        _publish_directory(
            directory / "runs",
            run_id,
            {
                "request.json": encoded(request),
                "events": None,
                "result.json": encoded(dict(schema_id=CONTRACT, run_id=run_id, status="RUNNING")),
            },
        )
        yield Run(directory / "runs" / run_id, run_id, clock)
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
