"""体积感知压缩候选、额度判定、版本领取和版本化队列验收。"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.services import service_for
from app.services.compaction_planner import MiB


M = MiB


def acceptance_files() -> list[dict]:
    return [
        {
            "path": f"partition-2026-11/small-{i:02d}.parquet",
            "size_bytes": 2 * M,
            "file_version": f"small-v{i}",
            "wait_seconds": 3600 + i * 60,
        }
        for i in range(8)
    ] + [
        {
            "path": "partition-2026-11/target-128.parquet",
            "size_bytes": 128 * M,
            "file_version": "large-v1",
            "wait_seconds": 7200,
        },
        {
            "path": "partition-2026-11/already-claimed.parquet",
            "size_bytes": 4 * M,
            "file_version": "claimed-v1",
            "claimed": True,
            "claimed_by": "maintenance-task-1",
            "wait_seconds": 7200,
        },
    ]


@pytest.fixture
def compaction():
    service = service_for("compaction")
    assert service is not None
    service.reset_state()
    yield service
    # 清理本测试同步到全局 store 的行，避免影响其他模块用例。
    from app.store import store
    rows = store.rows("compaction")
    rows[:] = [
        row for row in rows
        if not str(row.get("清单键", "")).startswith("compaction-acceptance-")
    ]
    service.reset_state()


def test_64mib_quota_recommends_eight_small_files_and_keeps_large_in_place(compaction):
    plan = compaction.estimate_candidates(
        {"inventory_version": "acceptance-64", "files": acceptance_files()},
        target_file_size_bytes=128 * M,
        rewrite_quota_bytes=64 * M,
        min_wait_seconds=600,
    )

    assert plan["ok"] is True
    assert plan["execution_scope"]["file_count"] == 8
    assert set(plan["execution_scope"]["file_versions"]) == {f"small-v{i}" for i in range(8)}

    reason_map = plan["exclusion_reason_map"]
    assert reason_map["partition-2026-11/target-128.parquet"]["reason_code"] == "target_size_reached"
    assert reason_map["partition-2026-11/already-claimed.parquet"]["reason_code"] == "already_claimed"

    group = plan["selected_groups"][0]
    assert group["file_count"] == 8
    assert group["input_bytes"] == 16 * M
    assert group["selected"] is True

    # 已领取文件仍在最终执行视野中，但不混入新候选执行范围。
    assert plan["claimed_file_versions"] == ["claimed-v1"]
    assert "claimed-v1" in plan["final_execution_scope"]["file_versions"]
    assert "claimed-v1" not in plan["execution_scope"]["file_versions"]


def test_8mib_quota_blocks_full_candidate_and_reports_reasons(compaction):
    plan = compaction.estimate_candidates(
        {"inventory_version": "acceptance-8", "files": acceptance_files()},
        target_file_size_bytes=128 * M,
        rewrite_quota_bytes=8 * M,
        min_wait_seconds=600,
    )

    assert plan["execution_scope"]["file_count"] == 0
    group = plan["candidate_groups"][0]
    assert group["selected"] is False
    assert "rewrite_quota_exceeded" in group["block_reasons"]

    small_decisions = [f for f in plan["files"] if f["file_version"].startswith("small-v")]
    assert small_decisions
    assert all(f["selected"] is False for f in small_decisions)
    assert all("rewrite_quota_exceeded" in f["reason_codes"] for f in small_decisions)

    reason_map = plan["exclusion_reason_map"]
    assert reason_map["large-v1"]["reason_code"] == "target_size_reached"
    assert reason_map["claimed-v1"]["reason_code"] == "already_claimed"
    assert plan["final_execution_scope"]["file_versions"] == ["claimed-v1"]


def test_concurrent_claims_allow_one_task_per_file_version(compaction):
    barrier = threading.Barrier(8)

    def claim(index: int) -> bool:
        barrier.wait()
        return compaction.claim_file_version("same-file-version", f"task-{index}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(claim, range(8)))

    assert results.count(True) == 1
    assert compaction.claim_file_version("same-file-version", "late-task") is False
    # 同一 task 的重试保持幂等。
    winner = next(f"task-{i}" for i, ok in enumerate(results) if ok)
    assert compaction.claim_file_version("same-file-version", winner) is True


def test_claimed_group_from_old_inventory_is_separate_from_next_candidates(compaction):
    files = [
        {"path": f"p/f{i}", "size_bytes": 2 * M, "file_version": f"version-{i}", "wait_seconds": 9999}
        for i in range(8)
    ]
    first = compaction.sync_inventory(
        {"inventory_version": "compaction-acceptance-v1", "table": "t", "partition": "p", "files": files},
        rewrite_quota_bytes=64 * M,
    )
    queue_key = first["queue"]["next_candidates"][0]["queue_id"]
    claimed = compaction.claim_queue_entry(queue_key, task_id="maintenance-winner")
    assert claimed["ok"] is True

    second_files = files + [
        {"path": "p/new", "size_bytes": M, "file_version": "new-version", "wait_seconds": 9999},
    ]
    second = compaction.sync_inventory(
        {"inventory_version": "compaction-acceptance-v2", "table": "t", "partition": "p", "files": second_files},
        rewrite_quota_bytes=64 * M,
    )
    assert second["ok"] is True

    queue = compaction.queue_snapshot()
    claimed_versions = {v for group in queue["claimed_groups"] for v in group["file_versions"]}
    candidate_versions = {v for group in queue["next_candidates"] for v in group["file_versions"]}
    assert {f"version-{i}" for i in range(8)} <= claimed_versions
    assert not ({f"version-{i}" for i in range(8)} & candidate_versions)

    v2_plan = second
    new_candidate_versions = set(v2_plan["execution_scope"]["file_versions"])
    assert f"version-0" not in new_candidate_versions
    assert "new-version" in new_candidate_versions
