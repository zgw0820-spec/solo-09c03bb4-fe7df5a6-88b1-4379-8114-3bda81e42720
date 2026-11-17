#!/usr/bin/env python3
"""End-to-end self-check against a running service.

    python3 scripts/selfcheck.py [base-url]

Verifies, over real HTTP:
1. health + two distinct models
2. switching the active model
3. version pinning: requests accepted before a switch keep the old version,
   requests accepted after it use the new one, and no batch mixes versions
4. batching: 16 emails -> batches of at most 4
5. deadlines: a slow email times out, its batch-mates still succeed, and the
   late result never flips the timeout back to success
6. early dispatch when the earliest deadline is near
"""

from __future__ import annotations

import sys
import time

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
SPAM = "Congratulations! You are a winner. Claim your free prize now."
HAM = "Hi team, attached is the quarterly report for our meeting tomorrow."
CRYPTO = "Limited offer! Invest in crypto today and double your money fast."

failures: list[str] = []


def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  ok   {message}")
    else:
        print(f"  FAIL {message}")
        failures.append(message)


def wait_job(client: httpx.Client, job_id: str, timeout: float = 10.0) -> dict:
    deadline = time.monotonic() + timeout
    state = {}
    while time.monotonic() < deadline:
        state = client.get(f"/api/jobs/{job_id}").json()
        if state["status"] in ("completed", "failed"):
            return state
        time.sleep(0.02)
    return state


def main() -> int:
    client = httpx.Client(base_url=BASE, timeout=30.0)

    print("1. health & models")
    health = client.get("/healthz").json()
    check(health["status"] == "ok", "service healthy")
    check({"v1", "v2"} <= set(health["models"]), "both v1 and v2 shipped")
    models = client.get("/api/models").json()["models"]
    sizes = {m["version"]: m["vocabulary_size"] for m in models}
    check(sizes["v1"] != sizes["v2"], f"models differ (v1={sizes['v1']}, v2={sizes['v2']} terms)")

    print("2. model switch")
    switched = client.post("/api/model", json={"version": "v2"}).json()
    check(switched["active"] == "v2", "active model switched to v2")
    check(client.get("/healthz").json()["active_model"] == "v2", "health reflects the switch")
    check(client.post("/api/model", json={"version": "nope"}).status_code == 404, "unknown version rejected")

    print("3. version pinning across a live switch")
    client.post("/api/model", json={"version": "v1"})
    job = client.post(
        "/api/jobs",
        json={
            "label": "pin",
            "emails": [
                {"text": SPAM, "deadline_ms": 900},
                {"text": SPAM, "deadline_ms": 900, "submit_after_ms": 260},
                {"text": SPAM, "deadline_ms": 900, "submit_after_ms": 420},
            ],
        },
    ).json()
    time.sleep(0.12)
    switch = client.post("/api/model", json={"version": "v2"}).json()
    check(switch["event"]["from"] == "v1" and switch["event"]["to"] == "v2", "switched v1 -> v2 mid-queue")
    pinned = wait_job(client, job["id"])
    versions = [e["model_version"] for e in pinned["entries"]]
    check(versions[0] == "v1", "email accepted before the switch kept v1")
    check(all(v == "v2" for v in versions[1:]), "emails accepted after the switch used v2")
    batches = client.get("/api/batches?limit=50").json()["batches"]
    for entry in pinned["entries"]:
        batch = next((b for b in batches if b["batch_id"] == entry["batch_id"]), None)
        if batch:
            expected = entry["model_version"]
            check(batch["model_version"] == expected, f"batch {batch['batch_id']} is pure {expected}")

    print("4. batching limits (16 emails)")
    client.post("/api/model", json={"version": "v1"})
    bulk = client.post(
        "/api/jobs",
        json={
            "label": "bulk16",
            "emails": [
                {"text": f"{SPAM} ref {i}", "deadline_ms": 1000}
                for i in range(16)
            ],
        },
    ).json()
    bulk_state = wait_job(client, bulk["id"])
    check(bulk_state["ok"] == 16, f"all 16 classified (ok={bulk_state['ok']})")
    bulk_ids = {e["batch_id"] for e in bulk_state["entries"]}
    check(len(bulk_ids) == 4, f"16 emails formed 4 batches (got {len(bulk_ids)})")
    sizes = [b["size"] for b in client.get("/api/batches?limit=50").json()["batches"] if b["batch_id"] in bulk_ids]
    check(max(sizes) <= 4 and sorted(sizes) == [4, 4, 4, 4], f"batch sizes {sorted(sizes)}")

    print("5. deadline timeout + late result cannot flip it")
    client.post("/api/model", json={"version": "v2"})
    timeout_job = client.post(
        "/api/classify",
        json={
            "label": "batch timeout",
            "emails": [
                {"text": SPAM, "deadline_ms": 100, "simulate_latency_ms": 600},
                {"text": CRYPTO, "deadline_ms": 1000},
            ],
        },
    ).json()
    late = next(e for e in timeout_job["entries"] if e["deadline_ms"] == 100)
    fast = next(e for e in timeout_job["entries"] if e["deadline_ms"] == 1000)
    check(late["status"] == "timeout", "slow email timed out")
    check(late["score"] is None, "timed-out email has no score")
    check(late["late_result_discarded"] is True, "late result explicitly discarded")
    check(fast["status"] == "ok", "batch-mate still succeeded")
    check(late["batch_id"] == fast["batch_id"], "they were in the same batch")
    time.sleep(0.7)
    reread = client.get(f"/api/jobs/{timeout_job['id']}").json()
    again = next(e for e in reread["entries"] if e["deadline_ms"] == 100)
    check(again["status"] == "timeout", "timeout still a timeout after the late thread finished")

    print("6. early dispatch when the earliest deadline is near")
    near = client.post(
        "/api/jobs",
        json={"label": "near", "emails": [{"text": SPAM, "deadline_ms": 150}]},
    ).json()
    near_state = wait_job(client, near["id"])
    entry = near_state["entries"][0]
    check(entry["status"] == "ok", "near-deadline request was dispatched early and scored")
    check(entry["latency_ms"] < 200, f"latency {entry['latency_ms']}ms < 200ms window")

    print("7. interleaved demo")
    demo = client.post("/api/demo/interleaved").json()
    demo_state = wait_job(client, demo["id"], timeout=10)
    old = [e for e in demo_state["entries"] if e["index"] <= 2]
    new = [e for e in demo_state["entries"] if e["index"] > 2]
    check({e["model_version"] for e in old} == {"v1"}, "demo old requests pinned to v1")
    check({e["model_version"] for e in new} == {"v2"}, "demo new requests used v2")
    slow = next(e for e in new if e["simulate_latency_ms"] > 0)
    check(slow["status"] == "timeout", "demo slow email timed out")
    check(client.get("/healthz").json()["active_model"] == "v2", "demo left v2 active")

    print("8. model retirement lifecycle")
    # Demo left v2 active; the active model cannot be retired until another
    # version takes over.
    check(
        client.post("/api/models/v2/retire").status_code == 409,
        "retiring the active model is refused",
    )
    # Start a full v2 batch with one slow email while v2 is still active, then
    # move the pointer to v1 and retire v2 so the late scoring thread is still
    # running when retirement begins.
    slow_job = client.post(
        "/api/jobs",
        json={
            "label": "retire-drain",
            "emails": [
                {"text": "SLOW crypto invest", "deadline_ms": 300, "simulate_latency_ms": 700},
                {"text": "parcel delivery fee", "deadline_ms": 900},
                {"text": "parcel delivery fee", "deadline_ms": 900},
                {"text": "parcel delivery fee", "deadline_ms": 900},
            ],
        },
    ).json()
    # Wait until the batch is actually executing (not merely queued) so every
    # entry has already pinned v2 before the pointer moves away.
    wait_until(
        client,
        lambda: client.get("/api/state").json()["worker"]["version_activity"]
        .get("v2", {})
        .get("executing", 0)
        >= 1,
    )
    client.post("/api/model", json={"version": "v1"})
    retire = client.post("/api/models/v2/retire")
    check(retire.status_code == 202, f"v2 retirement accepted (202), got {retire.status_code}")
    check(retire.json()["state"] == "retiring", "v2 enters retiring state")

    # Switch-back and new bindings are rejected while retiring.
    check(
        client.post("/api/model", json={"version": "v2"}).status_code == 409,
        "switch back to retiring v2 is refused",
    )
    check(
        client.post("/api/models/v2/retire").status_code == 409,
        "retiring v2 twice is refused",
    )
    state = client.get("/api/state").json()
    check(
        state["retirements"]["v2"]["activity"]["late_threads"] >= 1,
        "late scoring thread keeps v2 in retiring state",
    )
    # Wait specifically for the slow email to time out, and verify pinning
    # *before* the drain can finish and unload v2.
    late = None
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        slow_done = wait_job(client, slow_job["id"], timeout=2)
        late = next(e for e in slow_done["entries"] if e["simulate_latency_ms"] > 0)
        if late["status"] == "timeout":
            break
        time.sleep(0.05)
    check(late is not None and late["status"] == "timeout", "slow retired-model email timed out")
    check(late["model_version"] == "v2", "already accepted email kept its pinned v2")
    check(late["late_result_discarded"] is True, "late result discarded, timeout not flipped")

    # Once every queued/executing/late item drains, v2 unloads automatically.
    unloaded = wait_until(
        client,
        lambda: client.get("/api/state").json()["retirements"]["v2"]["state"] == "unloaded",
    )
    check(unloaded, "v2 unloaded only after the late thread finished")
    health = client.get("/healthz").json()
    check("v2" not in health["models"], "v2 removed from runtime registry")
    check(
        client.post("/api/model", json={"version": "v2"}).status_code == 409,
        "switching to an unloaded version stays refused",
    )
    views = {m["version"]: m for m in client.get("/api/models").json()["models"]}
    check(views["v2"]["state"] == "unloaded" and views["v2"]["unloaded_at"], "unloaded tombstone exposed")
    # v1 still classifies correctly.
    still = client.post(
        "/api/classify",
        json={"label": "after-retire", "emails": [{"text": SPAM, "deadline_ms": 800}]},
    ).json()
    check(still["ok"] == 1 and still["entries"][0]["model_version"] == "v1", "v1 classification intact after v2 unload")

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("all checks passed")
    return 0


def wait_until(client, predicate, timeout: float = 8.0, tick: float = 0.05) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(tick)
    return predicate()


if __name__ == "__main__":
    raise SystemExit(main())
