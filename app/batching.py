"""Single-worker, deadline-aware, version-partitioned batch scheduler.

Contract implemented here
-------------------------
* A request's model version is pinned by the caller at intake time.  The
  scheduler never re-resolves it, so requests accepted before a model switch
  keep the old model while newly accepted requests use the new one.
* Queue entries are grouped by pinned model version.  A batch never mixes
  versions.
* A batch holds at most ``max_batch_size`` (4) requests and waits at most
  ``batch_wait_ms`` (200 ms) counted from the oldest enqueue in that batch.
* If the earliest deadline inside a candidate batch gets close, the batch is
  dispatched early (``dispatch_guard_ms`` before that deadline) instead of
  waiting out the full window.
* Every request has its own expiry timer.  When its deadline passes the
  request is resolved as a timeout and removed from the queue, so it cannot
  occupy a slot in a later batch.
* Scoring inside a batch runs concurrently, so one slow email cannot starve
  its batch-mates.  Each request is checked against its *own* deadline.
  If scoring finishes after that deadline the request stays a timeout; a late
  prediction is discarded and can never flip it to success.  The other
  requests in the batch resolve independently against their own deadlines.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Iterable

from .model import LinearBoWModel, ModelRegistry, Prediction

logger = logging.getLogger(__name__)

Scorer = Callable[[LinearBoWModel, str], Prediction]


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _empty_counts() -> dict[str, int]:
    return {"submitted": 0, "completed_ok": 0, "timed_out": 0, "errored": 0, "batches": 0}


@dataclass
class WorkItem:
    request_id: str
    text: str
    model_version: str
    deadline: float  # time.monotonic() based
    enqueued_at: float
    submitted_at: float
    submitted_wall: str
    deadline_ms: int
    request_batch_id: str
    seq: int
    future: asyncio.Future
    simulated_latency_s: float = 0.0
    phase: str = "queued"  # queued | executing | resolved
    resolved: bool = False
    batch_id: str | None = None
    result: dict | None = None
    timer: asyncio.TimerHandle | None = None


class Worker:
    """One asyncio worker task that scores deadline-bound batches."""

    def __init__(
        self,
        registry: ModelRegistry,
        *,
        max_batch_size: int = 4,
        batch_wait_ms: float = 200.0,
        dispatch_guard_ms: float = 25.0,
        scorer: Scorer | None = None,
        history_limit: int = 200,
    ) -> None:
        if max_batch_size < 1:
            raise ValueError("max_batch_size must be >= 1")
        if batch_wait_ms < 0 or dispatch_guard_ms < 0:
            raise ValueError("wait/guard durations must be non-negative")
        self.registry = registry
        self.max_batch_size = int(max_batch_size)
        self.batch_wait_s = float(batch_wait_ms) / 1000.0
        self.dispatch_guard_s = float(dispatch_guard_ms) / 1000.0
        self._scorer: Scorer = scorer or (lambda model, text: model.predict(text))
        self.history_limit = int(history_limit)

        self._queues: dict[str, list[WorkItem]] = {}
        self._inflight: list[WorkItem] = []
        self._batch_tasks: set[asyncio.Task] = set()
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._closing = False
        self._seq = itertools.count(1)
        self._batch_seq = itertools.count(1)

        self.metrics: dict[str, int] = {
            "submitted": 0,
            "completed_ok": 0,
            "timed_out": 0,
            "errored": 0,
            "batches": 0,
            "late_results_discarded": 0,
        }
        self.metrics_by_version: dict[str, dict[str, int]] = {}
        self._batches: list[dict] = []

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._task is not None:
            return
        self._closing = False
        # Rebind per-loop primitives so a stopped worker can be restarted on a
        # fresh event loop (e.g. between test clients).
        self._wake = asyncio.Event()
        self._batch_tasks.clear()
        self._task = asyncio.create_task(self._run(), name="moderation-worker")
        logger.info(
            "worker started (max_batch=%d, wait=%.0fms, guard=%.0fms)",
            self.max_batch_size,
            self.batch_wait_s * 1000,
            self.dispatch_guard_s * 1000,
        )

    async def stop(self) -> None:
        self._closing = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        for batch_task in list(self._batch_tasks):
            batch_task.cancel()
        if self._batch_tasks:
            await asyncio.gather(*self._batch_tasks, return_exceptions=True)
            self._batch_tasks.clear()
        for item in self._unresolved():
            if item.phase == "executing":
                self._finalize(
                    item,
                    self._build_result(
                        item, status="error", reason="worker_stopped_during_batch"
                    ),
                )
            else:
                self._finalize(
                    item,
                    self._build_result(
                        item, status="timeout", reason="worker_stopped_before_dispatch"
                    ),
                )
        self._inflight.clear()
        logger.info("worker stopped")

    def _unresolved(self) -> list[WorkItem]:
        seen: list[WorkItem] = []
        for item in [*self._inflight, *[i for items in self._queues.values() for i in items]]:
            if not item.resolved and not item.future.done():
                seen.append(item)
        return seen

    # ------------------------------------------------------------------
    # submission
    # ------------------------------------------------------------------
    async def submit(
        self,
        *,
        text: str,
        model_version: str,
        deadline_ms: int,
        request_id: str,
        request_batch_id: str,
        latency_sim_ms: int = 0,
    ) -> dict:
        """Enqueue one request and wait for its (possibly timeout) result."""
        # Validate eagerly so a caller cannot pin an unknown model and the
        # timeout cannot be shorter/longer than the supported envelope.
        self.registry.get(model_version)
        if not 100 <= int(deadline_ms) <= 1000:
            raise ValueError("deadline_ms must be within [100, 1000]")

        loop = asyncio.get_running_loop()
        now = time.monotonic()
        item = WorkItem(
            request_id=request_id,
            text=text,
            model_version=model_version,
            deadline=now + deadline_ms / 1000.0,
            enqueued_at=now,
            submitted_at=now,
            submitted_wall=_iso_now(),
            deadline_ms=int(deadline_ms),
            request_batch_id=request_batch_id,
            seq=next(self._seq),
            future=loop.create_future(),
            simulated_latency_s=max(0.0, float(latency_sim_ms)) / 1000.0,
        )
        self._queues.setdefault(model_version, []).append(item)
        item.timer = loop.call_later(deadline_ms / 1000.0, self._on_deadline, item)
        self.metrics["submitted"] += 1
        self.metrics_by_version.setdefault(model_version, _empty_counts())["submitted"] += 1
        self._wake.set()
        return await item.future

    # ------------------------------------------------------------------
    # worker loop
    # ------------------------------------------------------------------
    async def _run(self) -> None:
        while not self._closing:
            self._wake.clear()
            now = time.monotonic()
            self._prune_expired(now)
            due, next_wake = self._plan(now)
            if due:
                # Single worker: take only the earliest-due batch.  Other due
                # batches stay queued (with their per-item deadline timers
                # still armed) and are re-planned once this one finishes, so
                # an executing batch can never silently swallow another
                # version's deadline.
                _due_at, version, batch = due[0]
                reserved = self._reserve(batch)
                if not reserved:
                    continue
                task = asyncio.create_task(
                    self._execute_batch(version, reserved),
                    name=f"batch-{version}",
                )
                self._batch_tasks.add(task)
                task.add_done_callback(self._batch_tasks.discard)
                await task
                continue
            if next_wake == math.inf:
                try:
                    await self._wake.wait()
                except asyncio.CancelledError:
                    raise
                continue
            delay = max(0.0, next_wake - now)
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive
                logger.exception("worker wait failed")

    def _plan(self, now: float) -> tuple[list[tuple[float, str, list[WorkItem]]], float]:
        """Return batches that are due now and the next wake-up time."""
        due: list[tuple[float, str, list[WorkItem]]] = []
        next_wake = math.inf
        for version, items in list(self._queues.items()):
            active = [it for it in items if not it.resolved and not it.future.done()]
            if not active:
                self._queues.pop(version, None)
                continue
            # Deadline-first selection: urgent work is never left behind an
            # arbitrary older request with a far-away deadline.
            active.sort(key=lambda it: (it.deadline, it.seq))
            # Anti-starvation: anything that has already waited out the full
            # batching window is dispatched in the next batch, even if a
            # continuous stream of tighter deadlines would otherwise keep
            # dereferencing it.  The rest of that batch is filled with other
            # queued items (EDF among them) so a batch is not under-filled
            # while work is available.
            starving = [it for it in active if now >= it.enqueued_at + self.batch_wait_s]
            if starving:
                batch = list(starving[: self.max_batch_size])
                if len(batch) < self.max_batch_size:
                    chosen = {id(it) for it in batch}
                    for candidate in active:
                        if len(batch) >= self.max_batch_size:
                            break
                        if id(candidate) not in chosen:
                            batch.append(candidate)
                            chosen.add(id(candidate))
                batch.sort(key=lambda it: (it.deadline, it.seq))
                due_at = now
            elif len(active) >= self.max_batch_size:
                batch = active[: self.max_batch_size]
                due_at = now
            else:
                batch = active
                window_end = min(it.enqueued_at for it in batch) + self.batch_wait_s
                urgent_at = min(it.deadline for it in batch) - self.dispatch_guard_s
                due_at = min(window_end, urgent_at)
            next_wake = min(
                next_wake,
                min(it.deadline for it in active),
                min(it.enqueued_at for it in active) + self.batch_wait_s,
            )
            if due_at <= now:
                due.append((due_at, version, batch))
            else:
                next_wake = min(next_wake, due_at)
        due.sort(key=lambda entry: entry[0])
        return due, next_wake

    def _prune_expired(self, now: float) -> None:
        for version, items in list(self._queues.items()):
            for item in list(items):
                if item.resolved or item.future.done():
                    self._remove(item)
                    continue
                if now >= item.deadline:
                    reason = (
                        "deadline_exceeded_during_batch"
                        if item.phase == "executing"
                        else "deadline_exceeded_before_dispatch"
                    )
                    self._finalize(
                        item,
                        self._build_result(
                            item,
                            status="timeout",
                            reason=reason,
                            batch_id=item.batch_id,
                        ),
                    )

    def _remove(self, item: WorkItem) -> None:
        items = self._queues.get(item.model_version)
        if items:
            try:
                items.remove(item)
            except ValueError:
                pass
            if not items:
                self._queues.pop(item.model_version, None)
        if item in self._inflight:
            self._inflight.remove(item)

    # ------------------------------------------------------------------
    # execution
    # ------------------------------------------------------------------
    def _reserve(self, candidates: Iterable[WorkItem]) -> list[WorkItem]:
        """Claim candidate items for a batch before it is dispatched.

        Removal happens here, synchronously, so the scheduler cannot pick the
        same item for a second batch while this one runs.  The per-item
        deadline timer is cancelled because from now on the executing batch
        enforces each item's deadline itself (via ``asyncio.wait_for`` plus an
        explicit post-check).  Keeping the timer alive would race the batch
        and make the result ambiguous.
        """
        reserved: list[WorkItem] = []
        for item in candidates:
            if item.resolved or item.future.done():
                self._remove(item)
                continue
            self._remove(item)
            if item.timer is not None:
                item.timer.cancel()
                item.timer = None
            item.phase = "executing"
            reserved.append(item)
        return reserved

    async def _execute_batch(self, version: str, batch: list[WorkItem]) -> None:
        if not batch:
            return
        # The batch was selected EDF, but re-sort defensively so deadline
        # order is guaranteed inside execution as well.
        batch.sort(key=lambda it: (it.deadline, it.seq))
        batch_id = f"b-{next(self._batch_seq):06d}"
        model = self.registry.get(version)
        self._inflight.extend(batch)
        for item in batch:
            item.batch_id = batch_id

        started_wall = _iso_now()
        started = time.monotonic()
        self.metrics["batches"] += 1
        self.metrics_by_version.setdefault(version, _empty_counts())["batches"] += 1

        try:
            await asyncio.gather(
                *(self._score_one(item, model, batch_id) for item in batch),
                return_exceptions=False,
            )
        finally:
            for item in batch:
                if item in self._inflight:
                    self._inflight.remove(item)

        record = {
            "batch_id": batch_id,
            "model_version": version,
            "started_at": started_wall,
            "completed_at": _iso_now(),
            "duration_ms": round((time.monotonic() - started) * 1000, 3),
            "size": len(batch),
            "ok": sum(1 for it in batch if it.result and it.result.get("status") == "ok"),
            "timeouts": sum(
                1 for it in batch if it.result and it.result.get("status") == "timeout"
            ),
            "errors": sum(
                1 for it in batch if it.result and it.result.get("status") == "error"
            ),
            "request_ids": [it.request_id for it in batch],
            "items": [self._history_item(it.result or {}) for it in batch],
        }
        self._batches.append(record)
        if len(self._batches) > self.history_limit:
            del self._batches[: len(self._batches) - self.history_limit]
        logger.info(
            "batch %s model=%s size=%d ok=%d timeouts=%d duration=%.1fms",
            batch_id,
            version,
            len(batch),
            record["ok"],
            record["timeouts"],
            record["duration_ms"],
        )

    async def _score_one(self, item: WorkItem, model: LinearBoWModel, batch_id: str) -> None:
        remaining = item.deadline - time.monotonic()
        try:
            prediction = await asyncio.wait_for(
                self._score_call(item, model), timeout=max(0.0, remaining)
            )
        except asyncio.TimeoutError:
            # Inference hit this request's own deadline.  The worker loop then
            # moves on to later batches.  (The scoring thread itself is not
            # cancellable, but its result is dropped on arrival and can never
            # flip the timeout below into a success.)
            self.metrics["late_results_discarded"] += 1
            if item.resolved:
                if item.result is not None and item.result.get("status") != "ok":
                    item.result["late_result_discarded"] = True
            else:
                self._finalize(
                    item,
                    self._build_result(
                        item,
                        status="timeout",
                        reason="deadline_exceeded_during_batch",
                        late_result_discarded=True,
                    ),
                )
            return
        except asyncio.CancelledError:
            if not item.resolved:
                self._finalize(
                    item,
                    self._build_result(
                        item, status="error", reason="worker_stopped_during_batch"
                    ),
                )
            raise
        except Exception:
            logger.exception("scoring failed for %s", item.request_id)
            if not item.resolved:
                self._finalize(
                    item,
                    self._build_result(item, status="error", reason="internal_error"),
                )
            return

        finished = time.monotonic()
        if item.resolved or item.future.done():
            # The deadline timer won the race while inference was running.
            # The late prediction must not turn the timeout into success.
            self.metrics["late_results_discarded"] += 1
            if item.result is not None and item.result.get("status") != "ok":
                item.result["late_result_discarded"] = True
            return
        if finished > item.deadline:
            # Inference came back after this request's own deadline.
            self.metrics["late_results_discarded"] += 1
            self._finalize(
                item,
                self._build_result(
                    item,
                    status="timeout",
                    reason="deadline_exceeded_during_batch",
                    late_result_discarded=True,
                ),
            )
            return

        assert isinstance(prediction, Prediction)
        self._finalize(
            item,
            self._build_result(
                item,
                status="ok",
                reason=None,
                score=prediction.score,
                label=prediction.label,
                batch_id=batch_id,
            ),
        )

    async def _score_call(self, item: WorkItem, model: LinearBoWModel):
        prediction = await asyncio.to_thread(self._scorer, model, item.text)
        if item.simulated_latency_s > 0:
            # Demo-only: emulate a slow inference call.  The deadline timer
            # and wait_for above still fire on schedule.
            await asyncio.sleep(item.simulated_latency_s)
        return prediction

    @staticmethod
    def _history_item(result: dict) -> dict:
        return {
            "request_id": result.get("request_id"),
            "status": result.get("status"),
            "score": result.get("score"),
            "label": result.get("label"),
            "latency_ms": result.get("latency_ms"),
            "reason": result.get("reason"),
        }

    # ------------------------------------------------------------------
    # resolution
    # ------------------------------------------------------------------
    def _on_deadline(self, item: WorkItem) -> None:
        if item.resolved or item.future.done():
            return
        reason = (
            "deadline_exceeded_during_batch"
            if item.phase == "executing"
            else "deadline_exceeded_before_dispatch"
        )
        self._finalize(
            item,
            self._build_result(
                item, status="timeout", reason=reason, batch_id=item.batch_id
            ),
        )

    def _build_result(
        self,
        item: WorkItem,
        *,
        status: str,
        reason: str | None,
        score: float | None = None,
        label: str | None = None,
        batch_id: str | None = None,
        late_result_discarded: bool = False,
    ) -> dict:
        now = time.monotonic()
        return {
            "request_id": item.request_id,
            "request_batch_id": item.request_batch_id,
            "batch_id": batch_id if batch_id is not None else item.batch_id,
            "model_version": item.model_version,
            "status": status,
            "reason": reason,
            "score": round(float(score), 6) if score is not None else None,
            "label": label,
            "threshold": self.registry.get(item.model_version).threshold,
            "deadline_ms": item.deadline_ms,
            "latency_ms": round((now - item.submitted_at) * 1000, 3),
            "phase": "resolved",
            "late_result_discarded": late_result_discarded,
            "submitted_at": item.submitted_wall,
            "completed_at": _iso_now(),
        }

    def _finalize(self, item: WorkItem, result: dict) -> None:
        if item.resolved:
            return
        item.resolved = True
        item.phase = "resolved"
        item.result = result
        if item.timer is not None:
            item.timer.cancel()
            item.timer = None
        self._remove(item)
        if result["status"] == "ok":
            self.metrics["completed_ok"] += 1
        elif result["status"] == "timeout":
            self.metrics["timed_out"] += 1
        elif result["status"] == "error":
            self.metrics["errored"] += 1
        bucket = self.metrics_by_version.setdefault(item.model_version, _empty_counts())
        counts_key = {"ok": "completed_ok", "timeout": "timed_out", "error": "errored"}.get(
            result["status"]
        )
        if counts_key:
            bucket[counts_key] += 1
        if not item.future.done():
            item.future.set_result(result)

    # ------------------------------------------------------------------
    # introspection
    # ------------------------------------------------------------------
    def pending_count(self, version: str | None = None) -> int:
        pending = self._unresolved()
        if version is None:
            return len(pending)
        return sum(1 for it in pending if it.model_version == version)

    def queued_count(self, version: str | None = None) -> int:
        """Items waiting for dispatch (excludes items already executing)."""
        return sum(
            1
            for version_key, items in self._queues.items()
            if version is None or version_key == version
            for item in items
            if not item.resolved and not item.future.done()
        )

    def recent_batches(self, limit: int = 20) -> list[dict]:
        return list(reversed(self._batches[-max(0, limit) :]))

    def snapshot(self) -> dict:
        pending = self._unresolved()
        by_version: dict[str, int] = {}
        for item in pending:
            by_version[item.model_version] = by_version.get(item.model_version, 0) + 1
        return {
            "pending": len(pending),
            "pending_by_version": by_version,
            "max_batch_size": self.max_batch_size,
            "batch_wait_ms": round(self.batch_wait_s * 1000, 3),
            "dispatch_guard_ms": round(self.dispatch_guard_s * 1000, 3),
            "metrics": dict(self.metrics),
            "metrics_by_version": {
                version: dict(counts) for version, counts in self.metrics_by_version.items()
            },
            "recent_batches": self.recent_batches(20),
        }
