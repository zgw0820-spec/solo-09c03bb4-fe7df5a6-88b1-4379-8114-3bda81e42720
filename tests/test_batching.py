"""Tests for the version-pinned, deadline-aware batch worker.

Run:  python3 -m pytest tests -q
"""

from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.batching import Worker  # noqa: E402
from app.model import LinearBoWModel, ModelRegistry, Prediction  # noqa: E402


def make_registry() -> ModelRegistry:
    v1 = LinearBoWModel(
        version="v1",
        vocabulary=["spam", "free", "meeting"],
        idf=[1.0, 1.0, 1.0],
        weights=[2.0, 2.0, -2.0],
        bias=-0.5,
        threshold=0.5,
    )
    v2 = LinearBoWModel(
        version="v2",
        vocabulary=["spam", "free", "meeting", "winner"],
        idf=[1.0, 1.0, 1.0, 1.0],
        weights=[3.0, 1.0, -1.5, 4.0],
        bias=-0.5,
        threshold=0.5,
    )
    return ModelRegistry({"v1": v1, "v2": v2}, "v1")


async def wait_for(predicate, timeout: float = 3.0, tick: float = 0.005):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(tick)
    return predicate()


@pytest.mark.asyncio
async def test_batch_waits_until_window_or_full():
    worker = Worker(make_registry(), max_batch_size=4, batch_wait_ms=200)
    await worker.start()
    try:
        started = time.monotonic()
        result = await asyncio.wait_for(
            worker.submit(
                text="free spam",
                model_version="v1",
                deadline_ms=1000,
                request_id="waiter",
                request_batch_id="wait",
            ),
            timeout=3,
        )
        elapsed = (time.monotonic() - started) * 1000
        assert result["status"] == "ok"
        # Single request: the batch must wait out the 200 ms window.
        assert 190 <= elapsed < 320, elapsed
        assert worker.recent_batches(1)[0]["size"] == 1
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_sixteen_emails_group_into_batches_of_four():
    worker = Worker(make_registry(), max_batch_size=4, batch_wait_ms=5)
    await worker.start()
    try:
        jobs = [
            worker.submit(
                text="free spam",
                model_version="v2",
                deadline_ms=900,
                request_id=f"bulk-{i}",
                request_batch_id="bulk",
            )
            for i in range(16)
        ]
        results = await asyncio.wait_for(asyncio.gather(*jobs), timeout=5)
        assert all(r["status"] == "ok" for r in results)
        batch_ids = {r["batch_id"] for r in results}
        assert len(batch_ids) == 4
        sizes = sorted(b["size"] for b in worker.recent_batches(50))
        assert sizes == [4, 4, 4, 4]
        for batch in worker.recent_batches(50):
            assert batch["model_version"] == "v2"
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_version_pinned_at_submission():
    """Queued old requests keep their version; new requests use the new one."""
    worker = Worker(make_registry(), max_batch_size=4, batch_wait_ms=150)
    await worker.start()
    try:
        old = asyncio.create_task(
            worker.submit(
                text="free spam",
                model_version="v1",
                deadline_ms=800,
                request_id="old-1",
                request_batch_id="job-old",
            )
        )
        await asyncio.sleep(0.02)
        # A model switch elsewhere must not retarget the already queued item.
        assert worker.pending_count("v1") == 1
        new = await asyncio.wait_for(
            worker.submit(
                text="free winner spam",
                model_version="v2",
                deadline_ms=800,
                request_id="new-1",
                request_batch_id="job-new",
            ),
            timeout=3,
        )
        old_result = await asyncio.wait_for(old, timeout=3)
        assert old_result["model_version"] == "v1"
        assert old_result["status"] == "ok"
        assert new["model_version"] == "v2"
        assert new["status"] == "ok"
        # v1 does not know "winner"; v2 does, so scores must differ.
        assert old_result["score"] != new["score"]
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_batches_never_mix_versions():
    worker = Worker(make_registry(), max_batch_size=4, batch_wait_ms=30)
    await worker.start()
    try:
        jobs = []
        for i in range(3):
            jobs.append(
                worker.submit(
                    text="free spam",
                    model_version="v1",
                    deadline_ms=600,
                    request_id=f"v1-{i}",
                    request_batch_id="mix",
                )
            )
            jobs.append(
                worker.submit(
                    text="winner spam",
                    model_version="v2",
                    deadline_ms=600,
                    request_id=f"v2-{i}",
                    request_batch_id="mix",
                )
            )
        results = await asyncio.wait_for(asyncio.gather(*jobs), timeout=4)
        assert len(results) == 6
        for batch in worker.recent_batches(50):
            versions = {
                item["request_id"].split("-")[0]
                for item in batch["items"]
                if item["status"] == "ok"
            }
            assert len(versions) <= 1, batch
            assert batch["size"] <= 4
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_batch_capped_at_four():
    worker = Worker(make_registry(), max_batch_size=4, batch_wait_ms=20)
    await worker.start()
    try:
        jobs = [
            worker.submit(
                text=f"free spam {i}",
                model_version="v1",
                deadline_ms=700,
                request_id=f"r-{i}",
                request_batch_id="cap",
            )
            for i in range(9)
        ]
        await asyncio.wait_for(asyncio.gather(*jobs), timeout=4)
        sizes = [b["size"] for b in worker.recent_batches(50)]
        assert sizes, "expected batches"
        assert max(sizes) <= 4
        # 9 items across at least 3 batches
        assert sum(sizes) >= 9
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_deadline_timeout_before_dispatch():
    """A request that expires while the single worker is busy never runs."""

    def slow_scorer(model: LinearBoWModel, text: str) -> Prediction:
        if "BLOCK" in text:
            time.sleep(0.30)
        return model.predict(text)

    worker = Worker(
        make_registry(), max_batch_size=4, batch_wait_ms=10, scorer=slow_scorer
    )
    await worker.start()
    try:
        # Occupy the single worker for ~300 ms.
        blocker = asyncio.create_task(
            worker.submit(
                text="BLOCK free spam",
                model_version="v1",
                deadline_ms=900,
                request_id="blocker",
                request_batch_id="dl",
            )
        )
        await asyncio.sleep(0.03)
        started = time.monotonic()
        result = await asyncio.wait_for(
            worker.submit(
                text="free spam",
                model_version="v1",
                deadline_ms=120,
                request_id="short",
                request_batch_id="dl",
            ),
            timeout=3,
        )
        elapsed = (time.monotonic() - started) * 1000
        assert result["status"] == "timeout"
        assert result["reason"] == "deadline_exceeded_before_dispatch"
        assert result["score"] is None
        assert 100 <= elapsed < 250, elapsed
        blocked = await asyncio.wait_for(blocker, timeout=3)
        assert blocked["status"] == "ok"
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_early_dispatch_when_deadline_is_near():
    """The guard fires the batch ~10ms before the nearest deadline."""
    worker = Worker(
        make_registry(), max_batch_size=4, batch_wait_ms=400, dispatch_guard_ms=20
    )
    await worker.start()
    try:
        started = time.monotonic()
        result = await asyncio.wait_for(
            worker.submit(
                text="free spam",
                model_version="v1",
                deadline_ms=150,
                request_id="urgent",
                request_batch_id="early",
            ),
            timeout=3,
        )
        elapsed = (time.monotonic() - started) * 1000
        assert result["status"] == "ok"
        # Dispatched before the 400 ms window, around deadline-20ms.
        assert elapsed < 200, elapsed
        batch = worker.recent_batches(1)[0]
        assert result["batch_id"] == batch["batch_id"]
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_expired_request_does_not_occupy_later_batch():
    """An expired queued request is dropped and never takes a later slot."""

    def slow_scorer(model: LinearBoWModel, text: str) -> Prediction:
        if "BLOCK" in text:
            time.sleep(0.25)
        return model.predict(text)

    worker = Worker(
        make_registry(),
        max_batch_size=2,
        batch_wait_ms=10,
        scorer=slow_scorer,
    )
    await worker.start()
    try:
        blocker = asyncio.create_task(
            worker.submit(
                text="BLOCK free spam",
                model_version="v1",
                deadline_ms=900,
                request_id="blocker",
                request_batch_id="slots",
            )
        )
        await asyncio.sleep(0.02)
        doomed = asyncio.create_task(
            worker.submit(
                text="free spam",
                model_version="v1",
                deadline_ms=110,
                request_id="doomed",
                request_batch_id="slots",
            )
        )
        # It expires while the worker is busy and must leave the queue.
        await asyncio.sleep(0.15)
        assert worker.queued_count("v1") == 0
        assert worker.pending_count("v1") == 1  # only the executing blocker
        doomed_result = await asyncio.wait_for(doomed, timeout=3)
        assert doomed_result["status"] == "timeout"
        assert doomed_result["reason"] == "deadline_exceeded_before_dispatch"
        assert doomed_result["batch_id"] is None

        live = [
            worker.submit(
                text="free spam",
                model_version="v1",
                deadline_ms=900,
                request_id=f"live-{i}",
                request_batch_id="slots",
            )
            for i in range(2)
        ]
        live_results = await asyncio.wait_for(asyncio.gather(*live), timeout=4)
        assert all(r["status"] == "ok" for r in live_results)
        # Both live requests fit into one fresh batch; the expired one did not
        # consume a slot in it.
        assert live_results[0]["batch_id"] == live_results[1]["batch_id"]
        for batch in worker.recent_batches(50):
            ids = [i["request_id"] for i in batch["items"]]
            if batch["batch_id"] == live_results[0]["batch_id"]:
                assert "doomed" not in ids
        assert (await asyncio.wait_for(blocker, timeout=3))["status"] == "ok"
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_late_result_never_flips_timeout_and_batchmates_succeed():
    """One email's scoring overruns; its batch-mates still succeed on time."""

    def slow_scorer(model: LinearBoWModel, text: str) -> Prediction:
        if "SLOW" in text:
            time.sleep(0.45)
        return model.predict(text)

    worker = Worker(
        make_registry(),
        max_batch_size=4,
        batch_wait_ms=10,
        scorer=slow_scorer,
    )
    await worker.start()
    try:
        tasks = [
            worker.submit(
                text="SLOW free spam",
                model_version="v1",
                deadline_ms=150,
                request_id="slow",
                request_batch_id="late",
            ),
            worker.submit(
                text="free spam",
                model_version="v1",
                deadline_ms=700,
                request_id="fast",
                request_batch_id="late",
            ),
        ]
        slow, fast = await asyncio.wait_for(asyncio.gather(*tasks), timeout=4)
        assert fast["status"] == "ok"
        assert slow["status"] == "timeout"
        assert slow["score"] is None
        assert slow["late_result_discarded"] is True
        # Their batch ids must be equal: they were batched together.
        assert slow["batch_id"] == fast["batch_id"]
        # Let the late thread finish and confirm nothing mutates afterwards.
        await asyncio.sleep(0.5)
        assert slow["status"] == "timeout"
        assert worker.metrics["late_results_discarded"] >= 1
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_mixed_deadlines_in_one_batch_resolve_independently():
    worker = Worker(make_registry(), max_batch_size=4, batch_wait_ms=10)
    await worker.start()
    try:
        tasks = [
            worker.submit(
                text="free spam",
                model_version="v2",
                deadline_ms=100,
                request_id="tight",
                request_batch_id="indep",
            ),
            worker.submit(
                text="winner spam",
                model_version="v2",
                deadline_ms=900,
                request_id="loose",
                request_batch_id="indep",
            ),
        ]
        tight, loose = await asyncio.wait_for(asyncio.gather(*tasks), timeout=4)
        assert tight["status"] in ("ok", "timeout")
        assert tight["latency_ms"] <= 200
        assert loose["status"] == "ok"
        assert loose["model_version"] == "v2"
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_worker_stop_resolves_waiters():
    worker = Worker(make_registry(), max_batch_size=4, batch_wait_ms=5000)
    await worker.start()
    task = asyncio.create_task(
        worker.submit(
            text="free spam",
            model_version="v1",
            deadline_ms=1000,
            request_id="stopper",
            request_batch_id="stop",
        )
    )
    await asyncio.sleep(0.02)
    await worker.stop()
    result = await asyncio.wait_for(task, timeout=3)
    assert result["status"] == "timeout"
    assert result["reason"] == "worker_stopped_before_dispatch"


def test_unknown_version_rejected():
    worker = Worker(make_registry())

    async def run():
        with pytest.raises(KeyError):
            await worker.submit(
                text="x",
                model_version="v9",
                deadline_ms=300,
                request_id="x",
                request_batch_id="x",
            )

    asyncio.run(run())


def test_model_scores_are_deterministic():
    registry = ModelRegistry.load_dir(ROOT / "models")
    text = "Congratulations! You are a winner. Claim your free prize."
    first = registry.get("v1").predict(text)
    second = registry.get("v1").predict(text)
    assert first == second
    assert 0.0 <= first.score <= 1.0
    # v2 must be a genuinely different model.
    assert registry.get("v1").weights.shape != registry.get("v2").weights.shape
    assert registry.get("v1").predict(text) != registry.get("v2").predict(text)
