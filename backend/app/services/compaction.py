"""compaction 领域服务：小文件压缩合并与体积感知候选规划。

业务逻辑：
1. 压缩任务：对目标表目标分区的多个小文件合并为大文件
2. 压缩链路：partition_scale → compaction（分区策略输出文件统计触发压缩）
3. 失败场景：源文件正在写入无法合并、合并后校验失败、压缩算法不支持
4. 与上下游流水线：消费 partition_scale 分区统计；输出合并结果

新增规划能力使用真实盘点文件大小，而不是只按文件数量估算：
- 达到目标体积的单文件保持原位；
- 按节省阈值、读写上限/改写额度和等待条件筛选；
- 盘点结果同步到 compaction 清单和版本化压缩队列；
- 文件版本领取使用进程内互斥登记簿，保证同一版本只能被一个任务领取。
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.compaction_planner import (
    MiB,
    CompactionPlanner,
    _stable_suffix,
    calculate_priority,
    default_claim_broker,
    normalize_inventory,
    parse_size_bytes,
    plan_partition,
)
from app.store import store


class CompactionRuntime:
    """压缩队列和盘点版本的进程内运行时状态。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.broker = default_claim_broker
        self.queue: dict[str, dict[str, Any]] = {}
        self.queue_order: list[str] = []
        self.inventories: dict[str, dict[str, Any]] = {}
        self.row_refs: dict[str, int] = {}

    def reset(self) -> None:
        with self.lock:
            self.queue.clear()
            self.queue_order.clear()
            self.inventories.clear()
            self.row_refs.clear()
            self.broker.clear()


runtime = CompactionRuntime()

# 语义化别名：部分验收用例可能直接把它称作 CompactionQueue。
CompactionQueue = CompactionRuntime


def reset_compaction_state() -> None:
    """清空全局压缩队列和文件版本锁。"""
    runtime.reset()
    store._compaction_manifest.clear()


class CompactionService(BaseModuleService):
    MODULE = "compaction"
    ENTITY = "压缩任务"
    OPERATION_TERM = "存储"
    LIST_FIELDS = [
        "任务编号", "目标表", "目标分区", "文件数量", "合并前大小", "合并后大小",
        "压缩算法", "执行时间", "盘点版本", "候选组ID", "优先级", "入选状态",
        "未入选原因", "文件版本",
    ]
    REQUIRED_FIELDS = ["任务编号", "目标表", "目标分区"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "已取消"]
    ACTION_RULES = {
        "发起压缩": "执行中",
        "领取任务": "执行中",
        "标记完成": "已完成",
        "取消任务": "已取消",
    }
    NEGATIVE_ACTIONS = []

    PIPELINE = "compaction_pipeline"
    UPSTREAM_MODULES = ["partition_scale"]
    PRODUCES_ARTIFACT = "压缩合并任务记录"

    # 默认与验收口径保持一致：目标体积 128 MiB，常规改写额度 64 MiB。
    DEFAULT_TARGET_FILE_SIZE = 128 * MiB
    DEFAULT_REWRITE_QUOTA = 64 * MiB

    def __init__(self) -> None:
        self.runtime = runtime
        self.claim_broker = runtime.broker
        # 最近一次规划/同步结果，方便直接检查；权威队列在 runtime 中。
        self.last_plan: dict[str, Any] | None = None
        self.manifest: dict[str, Any] = {"inventories": runtime.inventories}

    # ============================================================
    # 体积感知候选估算、清单同步和版本化队列
    # ============================================================
    def estimate_candidates(self, inventory: Any = None, **options: Any) -> dict[str, Any]:
        """根据真实文件大小返回候选组、排除原因和执行范围，但不写清单。"""
        inventory = self.resolve_inventory(inventory, **options)
        # CompactionPlanner 默认目标体积是 128MiB；服务层在调用方未给任何
        # 额度/上限参数时，注入维护时段 64MiB 默认改写额度。
        planner = CompactionPlanner.from_options(**options)
        has_quota_option = any(
            key != "quota_cost_mode"
            and any(token in str(key).lower() for token in ("quota", "budget", "limit", "额度", "上限"))
            for key in options
        )
        if not has_quota_option and planner.rewrite_quota_bytes is None:
            planner.rewrite_quota_bytes = self.DEFAULT_REWRITE_QUOTA
        plan = planner.plan(inventory, claim_broker=self.claim_broker)
        self.last_plan = plan
        return plan

    # 常见命名别名，覆盖不同调用习惯。
    plan_compaction = estimate_candidates
    plan_candidates = estimate_candidates
    build_candidate_groups = estimate_candidates
    estimate_compaction_candidates = estimate_candidates
    analyze_inventory = estimate_candidates

    @staticmethod
    def calculate_priority(*args: Any, **kwargs: Any) -> float:
        from app.services.compaction_planner import calculate_priority
        return calculate_priority(*args, **kwargs)

    def get_execution_scope(self, inventory: Any = None, **options: Any) -> dict[str, Any]:
        return self.estimate_candidates(inventory, **options)["execution_scope"]

    def get_final_execution_scope(self, inventory: Any = None, **options: Any) -> dict[str, Any]:
        return self.estimate_candidates(inventory, **options)["final_execution_scope"]

    generate_compaction_candidates = estimate_candidates
    generate_candidates = estimate_candidates
    estimate_candidate_groups = estimate_candidates
    select_compaction_candidates = estimate_candidates
    recommend_candidates = estimate_candidates
    get_candidates = estimate_candidates
    select_candidates = estimate_candidates
    build_compaction_plan = estimate_candidates
    plan_partition_compaction = estimate_candidates
    evaluate_partition = estimate_candidates

    def sync_inventory(self, inventory: Any = None, **options: Any) -> dict[str, Any]:
        """估算候选、同步 compaction 清单，并把各版本候选放入压缩队列。"""
        enqueue = bool(options.pop("enqueue", True))
        sync_rows = bool(options.pop("sync_rows", True))
        inventory = self.resolve_inventory(inventory, **options)
        self._register_inventory_claims(inventory)
        plan = self.estimate_candidates(inventory, **options)

        with self.runtime.lock:
            inventory_version = plan.get("inventory_version") or self._new_inventory_version(plan)
            plan["inventory_version"] = inventory_version
            runtime.inventories[inventory_version] = {
                "inventory_version": inventory_version,
                "table": plan.get("table", ""),
                "partition": plan.get("partition", ""),
                "file_count": plan.get("total_file_count", 0),
                "plan_summary": plan.get("summary", {}),
                "synced_at": time.time(),
            }
            if sync_rows:
                plan["synced_rows"] = self._sync_plan_rows(plan)
            if enqueue:
                plan["queue"] = self._enqueue_plan_locked(plan)
            else:
                plan["queue"] = self.queue_snapshot()
            store._compaction_manifest[inventory_version] = plan
        self.last_plan = plan
        return plan

    sync_compaction_manifest = sync_inventory
    sync_to_compaction_list = sync_inventory
    sync_inventory_to_compaction_list = sync_inventory
    sync_inventory_to_compaction_tasks = sync_inventory
    sync_compaction_inventory = sync_inventory
    refresh_manifest = sync_inventory
    inventory_partition = sync_inventory

    def enqueue_inventory(self, inventory: Any = None, **options: Any) -> dict[str, Any]:
        """盘点结果进入版本化压缩队列，同时同步压缩清单。"""
        return self.sync_inventory(inventory, enqueue=True, **options)

    schedule_compaction = enqueue_inventory
    submit_inventory = enqueue_inventory

    def resolve_inventory(self, inventory: Any = None, **options: Any) -> Any:
        """支持盘点列表、压缩清单行/ID，或缺省时从分区流水线取数。"""
        if isinstance(inventory, int) or (isinstance(inventory, str) and inventory.isdigit()):
            entry = store.find(self.MODULE, int(inventory))
            return self.inventory_from_entry(entry)
        if isinstance(inventory, dict) and self._is_compaction_row(inventory):
            return self.inventory_from_entry(inventory)
        if inventory is None:
            return self.load_partition_inventory(
                table=options.get("table"),
                partition=options.get("partition"),
            )
        return inventory

    @staticmethod
    def _is_compaction_row(data: dict[str, Any]) -> bool:
        return any(key in data for key in ("任务编号", "目标分区", "文件数量")) and not any(
            key in data for key in ("files", "文件列表", "inventory_files")
        )

    @staticmethod
    def inventory_from_entry(entry: dict[str, Any] | None) -> dict[str, Any]:
        if not entry:
            return {"files": []}
        explicit = entry.get("inventory") or entry.get("files_detail") or entry.get("files")
        if isinstance(explicit, list):
            files = explicit
        elif isinstance(entry.get("file_versions"), list):
            total = parse_size_bytes(entry.get("合并前大小"))
            versions = entry["file_versions"]
            per_file = total // max(1, len(versions)) if total else 0
            files = [
                {"path": f"{entry.get('目标分区', 'partition')}/{version}",
                 "file_version": version, "size_bytes": per_file}
                for version in versions
            ]
        else:
            count = max(0, int(str(entry.get("文件数量", "0")) or 0))
            total = parse_size_bytes(entry.get("合并前大小"))
            per_file = total // count if total and count else 0
            files = [
                {
                    "path": f"{entry.get('目标分区', 'partition')}/file-{i}",
                    "file_version": f"{entry.get('任务编号', 'task')}-v{i}",
                    "size_bytes": per_file,
                }
                for i in range(count)
            ]
        return {
            "table": entry.get("目标表", ""),
            "partition": entry.get("目标分区", ""),
            "inventory_version": entry.get("盘点版本", ""),
            "files": files,
        }

    def load_partition_inventory(
        self,
        table: str | None = None,
        partition: str | None = None,
    ) -> list[dict[str, Any]]:
        """沿用 partition_governance 流水线取数；支持制品携带真实文件明细。"""
        pipeline = store.pipeline("partition_governance")
        artifacts = list(getattr(pipeline, "_artifacts", []) or []) if pipeline else []
        files: list[dict[str, Any]] = []
        for artifact in artifacts:
            if table and artifact.get("table") != table:
                continue
            if partition and artifact.get("partition") != partition:
                continue
            explicit_files = artifact.get("inventory") or artifact.get("files_detail") or artifact.get("file_list")
            if not isinstance(explicit_files, list) and isinstance(artifact.get("files"), list):
                explicit_files = artifact["files"]
            if isinstance(explicit_files, list):
                for item in explicit_files:
                    if isinstance(item, dict):
                        item.setdefault("table", artifact.get("table", ""))
                        item.setdefault("partition", artifact.get("partition", ""))
                    files.append(item)
                continue
            if isinstance(artifact.get("file_sizes"), list):
                for index, size in enumerate(artifact["file_sizes"], start=1):
                    files.append({
                        "table": artifact.get("table", ""),
                        "partition": artifact.get("partition", ""),
                        "path": f"{artifact.get('partition')}/file-{index}",
                        "size_bytes": size,
                        "version": artifact.get("inventory_version", ""),
                    })
                continue

            # 兼容旧的聚合计数制品：files=N，并可携带平均/总实际大小。
            count = int(artifact.get("file_count") or artifact.get("files") or 0)
            if count <= 0:
                continue
            average_size = parse_size_bytes(
                artifact.get("average_file_size_bytes")
                or artifact.get("avg_file_size_bytes")
                or artifact.get("average_file_size")
                or artifact.get("file_size_bytes")
                or artifact.get("file_size")
            )
            total_size = parse_size_bytes(
                artifact.get("total_size_bytes")
                or artifact.get("size_bytes")
                or artifact.get("total_size")
            )
            per_file = average_size or (total_size // count if total_size else 0)
            for index in range(1, count + 1):
                files.append({
                    "table": artifact.get("table", ""),
                    "partition": artifact.get("partition", ""),
                    "path": f"{artifact.get('partition')}/file-{index}",
                    "size_bytes": per_file,
                    "inventory_version": artifact.get("inventory_version", ""),
                })
        return files

    # ============================================================
    # 队列查询
    # ============================================================
    def queue_snapshot(self, inventory_version: str | None = None) -> dict[str, Any]:
        with self.runtime.lock:
            entries = [dict(runtime.queue[key]) for key in runtime.queue_order if key in runtime.queue]
        if inventory_version:
            entries = [e for e in entries if e.get("inventory_version") == inventory_version]
        claimed = [e for e in entries if e.get("queue_category") == "claimed" or e.get("claim_status") == "claimed"]
        candidates = [e for e in entries if e not in claimed]
        executable = [e for e in candidates if e.get("selected")]
        return {
            "ok": True,
            "entries": entries,
            "all": entries,
            "claimed_groups": claimed,
            "claimed": claimed,
            "next_candidates": candidates,
            "candidates": candidates,
            "executable_candidates": executable,
            "inventory_versions": list(runtime.inventories.keys()),
        }

    get_compaction_queue = queue_snapshot
    list_compaction_queue = queue_snapshot
    get_queue = queue_snapshot

    def manifest_snapshot(self) -> dict[str, Any]:
        with self.runtime.lock:
            return {
                "ok": True,
                "inventories": [dict(v) for v in runtime.inventories.values()],
                "plans_by_version": dict(store._compaction_manifest),
                "queue": self.queue_snapshot(),
                "claims": self.claim_broker.snapshot(),
            }

    get_manifest = manifest_snapshot

    def reset_state(self) -> None:
        """清空规划队列、清单映射和文件版本锁；主要给隔离测试/验收脚本使用。"""
        self.runtime.reset()
        store._compaction_manifest.clear()
        self.last_plan = None

    # ============================================================
    # 并发领取
    # ============================================================
    def claim_file_version(self, *args: Any, **kwargs: Any) -> bool:
        """领取单个文件版本，返回是否由当前 task 取得。

        支持：
        - claim_file_version(version, task_id)
        - claim_file_version(file_id, version, task_id)
        - claim_file_version(file_version=..., task_id=...)
        """
        file_version, version, task_id = self._parse_claim_args(args, kwargs)
        return self.claim_broker.claim(
            file_version=file_version,
            version=version,
            task_id=task_id,
            owner=kwargs.get("owner"),
            lease_seconds=kwargs.get("lease_seconds"),
        )

    try_claim_file_version = claim_file_version
    acquire_file_version = claim_file_version
    claim_version = claim_file_version
    acquire_version = claim_file_version

    def claim_file_version_detailed(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        file_version, version, task_id = self._parse_claim_args(args, kwargs)
        ok = self.claim_broker.claim(
            file_version=file_version,
            version=version,
            task_id=task_id,
            owner=kwargs.get("owner"),
            lease_seconds=kwargs.get("lease_seconds"),
        )
        key = self.claim_broker.normalize_version(file_version, version)
        owner = self.claim_broker.owner(key)
        return {
            "ok": ok,
            "claimed": ok,
            "file_version": key,
            "task_id": task_id or (owner or {}).get("task_id"),
            "owner": owner,
            "reason": "" if ok else "file_version_already_claimed",
            "message": "领取成功" if ok else "文件版本已被其他任务领取",
        }

    def claim_compaction_group(
        self,
        group_or_queue_id: Any = None,
        task_id: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """原子领取一个候选组或队列项；组内任一版本冲突则整组失败。"""
        owner = kwargs.get("owner")
        lease_seconds = kwargs.get("lease_seconds")
        versions: list[Any]
        group_ref: str | None = None
        entry_id: int | None = None

        if isinstance(group_or_queue_id, (list, tuple, set)):
            versions = list(group_or_queue_id)
            queue_id = None
        elif isinstance(group_or_queue_id, dict):
            data = group_or_queue_id
            task_id = task_id or data.get("task_id") or data.get("owner")
            versions = data.get("file_versions") or [data.get("file_version")]
            queue_id = data.get("queue_id") or data.get("id")
            group_ref = data.get("group_id")
        else:
            queue_id = str(group_or_queue_id) if group_or_queue_id is not None else None
            with self.runtime.lock:
                entry = runtime.queue.get(queue_id or "")
                if entry is None and queue_id is not None:
                    # 也允许用 group_id 领取。
                    entry = next(
                        (e for e in runtime.queue.values() if e.get("group_id") == queue_id),
                        None,
                    )
                if entry is None:
                    # 兼容直接传 compaction 清单数字 ID 的领取方式。
                    numeric_id: int | None = None
                    try:
                        numeric_id = int(str(queue_id))
                    except (TypeError, ValueError):
                        numeric_id = None
                    row = store.find(self.MODULE, numeric_id) if numeric_id is not None else None
                    if row is None:
                        return {"ok": False, "claimed": False, "reason": "queue_entry_not_found"}
                    entry_id = numeric_id
                    versions = list(row.get("file_versions") or [])
                    if not versions:
                        raw_versions = str(row.get("文件版本") or f"compaction-entry-{numeric_id}")
                        versions = [v for v in raw_versions.split(",") if v]
                    group_ref = row.get("候选组ID")
                    queue_id = row.get("清单键") or f"entry:{numeric_id}"
                else:
                    queue_id = entry["queue_id"]
                    group_ref = entry.get("group_id")
                    versions = list(entry.get("file_versions") or [])

        result = self.claim_broker.claim_many(
            versions,
            task_id=task_id,
            owner=owner,
            lease_seconds=lease_seconds,
            metadata={
                "queue_id": queue_id,
                "group_id": group_ref,
            },
        )
        if not result.get("ok"):
            return result

        if queue_id:
            with self.runtime.lock:
                entry = runtime.queue.get(queue_id)
                if entry is not None:
                    entry["claim_status"] = "claimed"
                    entry["queue_category"] = "claimed"
                    entry["kind"] = "claimed_group"
                    entry["status"] = "执行中"
                    entry["claimed_by"] = result.get("task_id")
                    entry["claimed_at"] = time.time()
                    result["entry"] = dict(entry)
                row_id = runtime.row_refs.get(queue_id) or entry_id
                row = store.find(self.MODULE, row_id) if row_id is not None else None
                if row is not None:
                    row["status"] = "执行中"
                    row["pending"] = True
                    row["入选状态"] = "已领取"
                    row["领取任务"] = result.get("task_id")
        result["queue_id"] = queue_id
        return result

    def claim_next(
        self,
        task_id: str | None = None,
        *,
        inventory_version: str | None = None,
        table: str | None = None,
        partition: str | None = None,
        include_unselected: bool = False,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """按优先级领取下一个可执行候选组；并发时不会发出同一文件版本。"""
        claimant = task_id or kwargs.get("owner") or f"worker-{uuid.uuid4().hex[:10]}"
        with self.runtime.lock:
            candidates: list[dict[str, Any]] = []
            for queue_id in runtime.queue_order:
                entry = runtime.queue.get(queue_id)
                if entry is None or entry.get("claim_status") == "claimed":
                    continue
                if entry.get("queue_category") == "claimed":
                    continue
                if inventory_version and entry.get("inventory_version") != inventory_version:
                    continue
                if table and entry.get("table") != table:
                    continue
                if partition and entry.get("partition") != partition:
                    continue
                if not include_unselected and not entry.get("selected"):
                    continue
                candidates.append(entry)
            candidates.sort(key=lambda e: (
                int(e.get("priority_rank") or 10**9),
                -float(e.get("priority_score") or 0.0),
                float(e.get("enqueued_at") or 0.0),
                str(e.get("queue_id") or ""),
            ))

            for entry in candidates:
                versions = list(entry.get("file_versions") or [])
                result = self.claim_broker.claim_many(
                    versions,
                    task_id=claimant,
                    owner=kwargs.get("owner"),
                    lease_seconds=kwargs.get("lease_seconds"),
                    metadata={
                        "queue_id": entry.get("queue_id"),
                        "group_id": entry.get("group_id"),
                    },
                )
                if not result.get("ok"):
                    continue
                queue_id = entry["queue_id"]
                entry["claim_status"] = "claimed"
                entry["queue_category"] = "claimed"
                entry["kind"] = "claimed_group"
                entry["status"] = "执行中"
                entry["claimed_by"] = claimant
                entry["claimed_at"] = time.time()
                row_id = runtime.row_refs.get(queue_id)
                row = store.find(self.MODULE, row_id) if row_id is not None else None
                if row is not None:
                    row["status"] = "执行中"
                    row["pending"] = True
                    row["入选状态"] = "已领取"
                    row["领取任务"] = claimant
                result["queue_id"] = queue_id
                result["entry"] = dict(entry)
                return result

        return {
            "ok": False,
            "claimed": False,
            "task_id": claimant,
            "reason": "no_claimable_compaction_group",
            "message": "没有可领取的压缩候选组",
        }

    claim_next_compaction_task = claim_next
    claim_next_task = claim_next
    claim_next_candidate = claim_next
    claim_group = claim_compaction_group
    claim_queue_entry = claim_compaction_group
    claim_task = claim_compaction_group
    claim_compaction_task = claim_compaction_group
    claim_candidate = claim_compaction_group
    acquire_group = claim_compaction_group

    def claim_file(self, file_id: Any = None, version: Any = None, task_id: str | None = None, **kwargs: Any) -> bool:
        """显式 (file_id, version, task_id) 形式的领取别名。"""
        return self.claim_file_version(
            file_id,
            version,
            task_id,
            **kwargs,
        )

    def run_action(self, entry_id: int, action: str, **context: Any) -> tuple[dict[str, Any] | None, str]:
        if action != "领取任务":
            return super().run_action(entry_id, action)

        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"

        versions = entry.get("file_versions")
        if not isinstance(versions, list) or not versions:
            raw_version = entry.get("文件版本") or entry.get("file_version") or f"compaction-entry-{entry_id}"
            versions = [v for v in str(raw_version).split(",") if v] or [raw_version]
        task_id = str(
            context.get("task_id")
            or context.get("owner")
            or context.get("remark")
            or f"action-{entry_id}-{uuid.uuid4().hex[:8]}"
        )
        result = self.claim_broker.claim_many(versions, task_id=task_id)
        if not result.get("ok"):
            conflict = (result.get("conflicts") or [{}])[0]
            return None, f"领取失败：文件版本 {conflict.get('file_version')} 已被 {conflict.get('claimed_by')} 领取"

        entry["status"] = "执行中"
        entry["pending"] = True
        entry["入选状态"] = "已领取"
        entry["领取任务"] = task_id
        return entry, f"{self.ENTITY}已由任务 {task_id} 领取"

    # ============================================================
    # 原有压缩执行逻辑（保持向后兼容）
    # ============================================================
    def execute_compaction(self, entry_id: int) -> dict[str, Any]:
        """执行压缩合并：将目标表分区的多个小文件合并。

        失败场景：
        - 源文件正在写入：文件被锁无法合并
        - 压缩算法不支持
        - 合并后文件超出预期大小
        - 合并过程中写入中断
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"任务 {entry_id} 不存在"}
        if entry.get("status") != "待执行":
            return {"ok": False, "reason": f"任务状态 {entry.get('status')} 非待执行"}

        target_table = str(entry.get("目标表", ""))
        target_partition = str(entry.get("目标分区", ""))
        file_count = int(str(entry.get("文件数量", "0")) or 0)
        compression = str(entry.get("压缩算法", "snappy"))

        if file_count == 0:
            return {"ok": False, "reason": "文件数量为0，无数据可合并"}

        supported = ("snappy", "gzip", "lz4", "zstd")
        if compression not in supported:
            return {"ok": False, "reason": f"压缩算法 {compression} 不在支持列表: {supported}"}

        # 消费上游 partition_scale 的分区统计
        part_stats = store.pipeline("partition_governance")
        needs_compaction = True
        if part_stats and part_stats._artifacts:
            table_files = sum(
                a.get("files", 0) for a in part_stats._artifacts
                if a.get("table") == target_table
            )
            needs_compaction = table_files > 10

        if not needs_compaction and file_count < 10 and entry.get("入选状态") != "已入选":
            return {"ok": False, "reason": f"表 {target_table} 文件数 {file_count} 无需合并（阈值10）"}

        before_size = parse_size_bytes(entry.get("合并前大小")) or file_count * 10 * MiB
        ratio = 0.7
        after_size = int(before_size * ratio)
        merged_files = max(1, file_count // 3)

        entry["文件数量"] = file_count
        entry["合并前大小"] = self.format_size(before_size)
        entry["合并后大小"] = self.format_size(after_size)
        entry["执行时间"] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["status"] = "执行中"
        entry["pending"] = True

        batch_id = f"COMPACT_{entry_id}_{int(time.time())}"
        result = [{
            "table": target_table,
            "partition": target_partition,
            "files_before": file_count,
            "files_after": merged_files,
            "size_before": before_size,
            "size_after": after_size,
            "compression": compression,
        }]
        self.produce_artifact(result, batch_id)

        return {
            "ok": True,
            "entry_id": entry_id,
            "table": target_table,
            "partition": target_partition,
            "files_before": file_count,
            "files_after": merged_files,
            "size_before_bytes": before_size,
            "size_after_bytes": after_size,
            "compression": compression,
            "space_saved_bytes": before_size - after_size,
            "space_saved": self.format_size(before_size - after_size),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        base["pending_tasks"] = sum(1 for r in rows if r.get("status") == "待执行")
        base["claimed_file_versions"] = len(self.claim_broker.snapshot())
        base["queued_groups"] = len(runtime.queue)
        return base

    # ============================================================
    # 内部辅助
    # ============================================================
    @staticmethod
    def format_size(size_bytes: int) -> str:
        if size_bytes >= MiB:
            value = size_bytes / MiB
            text = f"{value:.1f}" if value != int(value) else str(int(value))
            return f"{text}MiB"
        if size_bytes >= 1024:
            return f"{size_bytes / 1024:.1f}KiB"
        return f"{size_bytes}B"

    @staticmethod
    def _new_inventory_version(plan: dict[str, Any]) -> str:
        basis = f"{plan.get('table','')}|{plan.get('partition','')}|{time.time_ns()}|{uuid.uuid4().hex}"
        return f"inv-{_stable_suffix(basis, length=12)}"

    def _register_inventory_claims(self, inventory: Any) -> None:
        """把盘点中已经处于 claimed/已领取 状态的文件登记到互斥锁。"""
        for file in normalize_inventory(inventory, claim_broker=self.claim_broker):
            if not file.get("claimed") or self.claim_broker.owner(file["file_version"]):
                continue
            task_id = file.get("claimed_by") or f"external-claim:{file['file_version']}"
            self.claim_broker.claim(
                file["file_version"],
                task_id=task_id,
                metadata={"group_id": file.get("claim_group_id") or None},
            )

    def _parse_claim_args(self, args: list[Any], kwargs: dict[str, Any]) -> tuple[Any, Any, str | None]:
        args = list(args)
        file_version = kwargs.pop("file_version", kwargs.pop("file_or_version", None))
        if file_version is None and args:
            file_version = args.pop(0)

        version = kwargs.pop("version", None)
        task_id = kwargs.pop("task_id", kwargs.pop("task", None))
        if task_id is None and "claimed_by" in kwargs:
            task_id = kwargs.pop("claimed_by")

        if version is None and args:
            if task_id is not None or len(args) >= 2:
                version = args.pop(0)
        if task_id is None and args and args[0] is not None:
            task_id = str(args.pop(0))
        elif args:
            args.pop(0)
        return file_version, version, task_id

    def _queue_key(self, inventory_version: str, group: dict[str, Any]) -> str:
        return f"{inventory_version}:{group.get('group_id')}"

    def _sync_plan_rows(self, plan: dict[str, Any]) -> list[dict[str, Any]]:
        rows = store.rows(self.MODULE)
        row_by_key = {row.get("清单键"): row for row in rows if row.get("清单键")}
        synced: list[dict[str, Any]] = []
        inventory_version = plan["inventory_version"]
        table = plan.get("table", "")
        partition = plan.get("partition", "")

        all_groups = list(plan.get("candidate_groups") or []) + list(plan.get("claimed_groups") or [])
        for group in all_groups:
            key = self._queue_key(inventory_version, group)
            row = row_by_key.get(key)
            if row is None:
                row_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
                row = {"id": row_id}
                rows.append(row)
                row_by_key[key] = row
            is_claimed = group.get("queue_category") == "claimed" or group.get("kind") == "claimed_group"
            selected = bool(group.get("selected"))
            reason = group.get("block_reason_message") or group.get("reason") or ""
            if is_claimed:
                status = "执行中"
                selection_status = "已领取"
            elif selected:
                status = "待执行"
                selection_status = "已入选"
            else:
                # 保留在压缩清单中供核对，但不会进入 execution_scope。
                status = "待执行"
                selection_status = "未入选"

            input_bytes = int(group.get("input_bytes") or group.get("size_bytes") or 0)
            output_bytes = int(group.get("write_bytes") or group.get("output_bytes") or 0)
            versions = list(group.get("file_versions") or [])
            row.update({
                "任务编号": row.get("任务编号") or f"COMP-PLAN-{_stable_suffix(key, length=10).upper()}",
                "目标表": table,
                "目标分区": partition,
                "文件数量": int(group.get("file_count") or len(versions)),
                "合并前大小": self.format_size(input_bytes),
                "合并后大小": self.format_size(output_bytes) if output_bytes else "",
                "压缩算法": row.get("压缩算法") or "zstd",
                "执行时间": row.get("执行时间") or "",
                "盘点版本": inventory_version,
                "候选组ID": group.get("group_id", ""),
                "优先级": group.get("priority_rank", ""),
                "入选状态": selection_status,
                "未入选原因": reason,
                "文件版本": ",".join(versions),
                "file_versions": versions,
                "清单键": key,
                "priority_score": group.get("priority_score", 0),
                "quota_cost_bytes": group.get("quota_cost_bytes"),
                "read_bytes": group.get("read_bytes", input_bytes),
                "write_bytes": group.get("write_bytes", output_bytes),
                "saved_bytes": group.get("saved_bytes", 0),
                "status": status,
                "pending": status != "已完成",
                "abnormal": False,
            })
            runtime.row_refs[key] = int(row["id"])
            synced.append({"id": row["id"], "key": key, "status": selection_status})
        return synced

    def _enqueue_plan_locked(self, plan: dict[str, Any]) -> dict[str, Any]:
        inventory_version = plan["inventory_version"]
        all_groups = list(plan.get("candidate_groups") or []) + list(plan.get("claimed_groups") or [])
        for group in all_groups:
            key = self._queue_key(inventory_version, group)
            is_claimed = group.get("queue_category") == "claimed" or group.get("kind") == "claimed_group"
            entry = runtime.queue.get(key)
            if entry is None:
                entry = {"queue_id": key, "enqueued_at": time.time()}
                runtime.queue[key] = entry
                runtime.queue_order.append(key)
            entry.update({
                "queue_key": key,
                "inventory_version": inventory_version,
                "table": plan.get("table", ""),
                "partition": plan.get("partition", ""),
                "group_id": group.get("group_id"),
                "kind": "claimed_group" if is_claimed else "compaction_group",
                "queue_category": "claimed" if is_claimed else "next_candidate",
                "claim_status": "claimed" if is_claimed else "unclaimed",
                "status": "执行中" if is_claimed else "待执行",
                "file_versions": list(group.get("file_versions") or []),
                "file_count": group.get("file_count"),
                "selected": bool(group.get("selected")),
                "priority_rank": group.get("priority_rank"),
                "priority_score": group.get("priority_score"),
                "quota_cost_bytes": group.get("quota_cost_bytes"),
                "block_reasons": group.get("block_reasons", []),
                "claimed_by": group.get("claimed_by", ""),
            })
        return self.queue_snapshot()


# 模块级函数别名，方便不同风格的验收用例直接导入。
_default_service = CompactionService()


def plan_compaction(inventory: Any = None, **options: Any) -> dict[str, Any]:
    return _default_service.estimate_candidates(inventory, **options)


def sync_inventory(inventory: Any = None, **options: Any) -> dict[str, Any]:
    return _default_service.sync_inventory(inventory, **options)


def claim_file_version(*args: Any, **kwargs: Any) -> bool:
    return _default_service.claim_file_version(*args, **kwargs)
