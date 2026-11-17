"""Model retirement tests: drain semantics, lifecycle and switch/retire races."""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import main as main_module  # noqa: E402


@pytest.fixture()
def client():
    main_module.reset_runtime()
    main_module.worker.metrics.update(
        {key: 0 for key in main_module.worker.metrics}
    )
    with TestClient(main_module.app) as test_client:
        yield test_client


def wait_until(predicate, timeout: float = 8.0, tick: float = 0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(tick)
    return predicate()


def wait_job(client, job_id, timeout: float = 10.0):
    def _poll():
        state = client.get(f"/api/jobs/{job_id}").json()
        return state if state["status"] in ("completed", "failed") else None

    assert wait_until(lambda: _poll() is not None, timeout=timeout)
    return _poll()


# ---------------------------------------------------------------------
def test_cannot_retire_active_model(client):
    assert client.get("/healthz").json()["active_model"] == "v1"
    r = client.post("/api/models/v1/retire")
    assert r.status_code == 409
    assert "现行模型" in r.json()["detail"]
    # v1 stays loaded and usable.
    assert "v1" in client.get("/healthz").json()["models"]


def test_retire_unknown_and_double_retire_rejected(client):
    assert client.post("/api/models/v9/retire").status_code == 404

    client.post("/api/model", json={"version": "v2"})
    first = client.post("/api/models/v1/retire")
    assert first.status_code == 202
    assert first.json()["state"] == "retiring"

    again = client.post("/api/models/v1/retire")
    assert again.status_code == 409


def test_retiring_model_cannot_be_switched_back(client):
    client.post("/api/model", json={"version": "v2"})
    client.post("/api/models/v1/retire")

    blocked = client.post("/api/model", json={"version": "v1"})
    assert blocked.status_code == 409
    assert client.get("/healthz").json()["active_model"] == "v2"


def test_retirement_drains_queued_and_executing_then_unloads(client):
    # v1 active; occupy it with work carrying generous deadlines so it is
    # still around when v1 is retired.
    client.post("/api/model", json={"version": "v1"})
    job = client.post(
        "/api/jobs",
        json={
            "label": "drain",
            "emails": [
                {"text": "free spam winner", "deadline_ms": 900},
                {"text": "meeting notes attached", "deadline_ms": 900},
            ],
        },
    ).json()
    # Make sure at least the first entry was accepted on v1 before retiring.
    assert wait_until(
        lambda: client.get(f"/api/jobs/{job['id']}").json()["entries"][0][
            "model_version"
        ]
        == "v1"
    )

    client.post("/api/model", json={"version": "v2"})
    retire = client.post("/api/models/v1/retire").json()
    assert retire["version"] == "v1"

    state = client.get("/api/state").json()
    assert state["retirements"]["v1"]["state"] in ("retiring", "unloaded")

    # Once everything settles, v1 must be unloaded and disappear from the
    # runtime registry, while the retired job still reports v1 results.
    assert wait_until(
        lambda: client.get("/api/state").json()["retirements"]["v1"]["state"]
        == "unloaded"
    )
    health = client.get("/healthz").json()
    assert "v1" not in health["models"]

    done = wait_job(client, job["id"])
    assert done["status"] == "completed"
    assert all(e["model_version"] == "v1" for e in done["entries"])
    assert all(e["status"] in ("ok", "timeout") for e in done["entries"])

    models = {m["version"]: m for m in client.get("/api/models").json()["models"]}
    assert models["v1"]["state"] == "unloaded"
    assert models["v1"]["unloaded_at"]
    assert models["v2"]["state"] == "loaded"


def test_late_scoring_thread_keeps_model_loaded_until_it_finishes(client):
    """A timed-out but still-running thread delays the unload."""
    client.post("/api/model", json={"version": "v1"})
    # A full batch (4) dispatches immediately, so the slow scoring thread is
    # already running when retirement starts.  It times out at 300ms but the
    # underlying thread sleeps ~700ms.
    slow_job = client.post(
        "/api/jobs",
        json={
            "label": "late-thread",
            "emails": [
                {
                    "text": "SLOW free spam winner",
                    "deadline_ms": 300,
                    "simulate_latency_ms": 700,
                },
                {"text": "meeting attached", "deadline_ms": 900},
                {"text": "meeting attached", "deadline_ms": 900},
                {"text": "meeting attached", "deadline_ms": 900},
            ],
        },
    ).json()
    # Let the batch dispatch and the slow thread start before retiring.
    time.sleep(0.06)
    client.post("/api/model", json={"version": "v2"})
    retire = client.post("/api/models/v1/retire").json()
    assert retire["activity"]["total"] >= 1

    # Wait for the slow request to time out while its thread is still alive.
    assert wait_until(
        lambda: client.get(f"/api/jobs/{slow_job['id']}").json()["entries"][0][
            "status"
        ]
        == "timeout",
        timeout=3,
    )
    state = client.get("/api/state").json()
    assert state["retirements"]["v1"]["state"] == "retiring"
    assert state["retirements"]["v1"]["activity"]["late_threads"] >= 1

    # After the late thread returns, unload happens and the timeout stands.
    assert wait_until(
        lambda: client.get("/api/state").json()["retirements"]["v1"]["state"]
        == "unloaded",
        timeout=5,
    )
    done = wait_job(client, slow_job["id"])
    late = next(e for e in done["entries"] if e["deadline_ms"] == 300)
    assert late["status"] == "timeout"
    assert late["score"] is None
    assert late["late_result_discarded"] is True


def test_new_mail_never_binds_to_retired_version(client):
    # A job whose second email is accepted (submit_after) only after the
    # version it would have pinned has been retired must not bind to it.
    client.post("/api/model", json={"version": "v1"})
    job = client.post(
        "/api/jobs",
        json={
            "label": "race",
            "emails": [
                {
                    "text": "free spam",
                    "deadline_ms": 900,
                    "submit_after_ms": 250,
                },
                {
                    "text": "free spam",
                    "deadline_ms": 900,
                    "submit_after_ms": 450,
                },
            ],
        },
    ).json()
    # Wait until the first entry is accepted on v1.
    assert wait_until(
        lambda: client.get(f"/api/jobs/{job['id']}").json()["entries"][0][
            "model_version"
        ]
        == "v1"
    )
    client.post("/api/model", json={"version": "v2"})
    client.post("/api/models/v1/retire")

    done = wait_job(client, job["id"])
    first, second = done["entries"]
    assert first["model_version"] == "v1"  # accepted before retirement
    # The second entry is accepted after v1 retired; the active pointer is now
    # v2, so it must either run on v2 or, if blocked, be refused -- never v1.
    assert second["model_version"] != "v1"
    assert second["status"] in ("ok", "timeout", "error")


def test_submission_rejected_when_active_version_is_retired(client):
    # Drive the active pointer onto v1, then retire it via a second active
    # model.  A demo-style delayed intake must refuse rather than bind v1.
    client.post("/api/model", json={"version": "v2"})
    client.post("/api/models/v1/retire")
    # The active model is v2, so normal intake still works; force the blocked
    # check directly through the worker seam as well.
    ok = client.post(
        "/api/jobs",
        json={"label": "v2 mail", "emails": [{"text": "hello", "deadline_ms": 500}]},
    )
    assert ok.status_code == 202

    main_module.active_version = "v1"  # simulate a stale pointer
    refused = client.post(
        "/api/jobs",
        json={"label": "v1 mail", "emails": [{"text": "hello", "deadline_ms": 500}]},
    )
    assert refused.status_code == 409
    assert "退役" in refused.json()["detail"]


def test_switch_and_retire_first_writer_wins(client):
    """Whichever decision acquires the ops lock first is the one that stands.

    The app runs its own event loop inside the TestClient portal, so the
    contended coroutines are scheduled there rather than on the pytest loop.
    """
    import asyncio

    from fastapi import HTTPException

    from app.main import ops_lock, retire_model, switch_model
    import app.main as m
    portal = client.portal

    async def contend():
        results: list[tuple[str, int]] = []

        async def do_retire():
            try:
                await retire_model("v1")
                results.append(("retire", 202))
            except HTTPException as exc:
                results.append(("retire", exc.status_code))

        async def do_switch():
            try:
                await switch_model(m.SwitchIn(version="v1"))
                results.append(("switch", 200))
            except HTTPException as exc:
                results.append(("switch", exc.status_code))

        # Hold the lock, queue both contenders, then release so the retire
        # coroutine (created first) acquires it before the switch coroutine.
        await ops_lock.acquire()
        retire_task = asyncio.create_task(do_retire())
        switch_task = asyncio.create_task(do_switch())
        await asyncio.sleep(0.02)
        ops_lock.release()
        await asyncio.gather(retire_task, switch_task)
        return results

    # Case 1: v1 still active -> retire loses (409), same-version switch is a
    # permitted no-op (200).
    client.post("/api/model", json={"version": "v1"})
    results = portal.call(contend)
    assert ("retire", 409) in results
    assert ("switch", 200) in results

    # Case 2: v2 active, retire v1 wins first -> the switch-back to v1 is
    # explicitly refused and v2 stays active.
    client.post("/api/model", json={"version": "v2"})
    results = portal.call(contend)
    assert ("retire", 202) in results
    assert ("switch", 409) in results
    assert client.get("/healthz").json()["active_model"] == "v2"
    assert wait_until(
        lambda: client.get("/api/state").json()["retirements"]["v1"]["state"]
        == "unloaded"
    )


def test_unloaded_version_rejected_at_worker_and_api(client):
    client.post("/api/model", json={"version": "v2"})
    client.post("/api/models/v1/retire")
    assert wait_until(
        lambda: client.get("/api/state").json()["retirements"]["v1"]["state"]
        == "unloaded"
    )

    # Switching to an unloaded version is a hard 409.
    r = client.post("/api/model", json={"version": "v1"})
    assert r.status_code == 409

    # The worker itself refuses any direct submission for the missing model.
    from app.batching import ModelRetiredError

    async def probe():
        with pytest.raises((ModelRetiredError, KeyError)):
            await main_module.worker.submit(
                text="x",
                model_version="v1",
                deadline_ms=300,
                request_id="probe",
                request_batch_id="probe",
            )

    client.portal.call(probe)
