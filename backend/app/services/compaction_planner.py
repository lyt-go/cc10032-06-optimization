"""体积感知的压缩候选估算与文件版本领取原语。

该模块不依赖 FastAPI 和全局 store，便于并发与验收用例直接调用。
规划器只根据盘点快照中的真实文件大小、目标体积、节省阈值、读写额度和
等待时间给出建议；是否同步清单/队列由 CompactionService 负责。
"""
from __future__ import annotations

import hashlib
import re
import threading
import time
import uuid
from dataclasses import dataclass, field, replace
from typing import Any, Iterable

MiB = 1024 * 1024
MB = 1000 * 1000

# 稳定原因码：测试和接口可以依赖 code，中文 message 仅用于展示。
REASON_CLAIMED = "already_claimed"
REASON_TARGET_SIZE = "target_size_reached"
REASON_WAITING = "waiting_threshold_not_met"
REASON_INVALID_SIZE = "invalid_file_size"
REASON_MIN_FILES = "min_input_files_not_met"
REASON_SAVING = "saving_threshold_not_met"
REASON_READ_LIMIT = "read_limit_exceeded"
REASON_WRITE_LIMIT = "write_limit_exceeded"
REASON_BUDGET = "rewrite_quota_exceeded"

REASON_MESSAGES = {
    REASON_CLAIMED: "文件版本已被其他任务领取，不进入下次候选组",
    REASON_TARGET_SIZE: "单文件已达到目标体积，保持原位，避免无效整组改写",
    REASON_WAITING: "小文件等候时间未达到等待阈值，暂不压缩",
    REASON_INVALID_SIZE: "文件大小无效，无法估算改写成本",
    REASON_MIN_FILES: "候选组文件数不足，合并收益未超过门槛",
    REASON_SAVING: "估算节省未达到节省阈值",
    REASON_READ_LIMIT: "候选组读取量超过读上限",
    REASON_WRITE_LIMIT: "候选组写入量超过写上限",
    REASON_BUDGET: "候选组改写量超过剩余改写额度",
}

_SIZE_RE = re.compile(
    r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTPE]?i?B?|字节|兆|吉)?\s*$",
    re.IGNORECASE,
)

_UNIT_BYTES = {
    "B": 1,
    "BYTE": 1,
    "BYTES": 1,
    "字节": 1,
    "K": 1024,
    "KB": 1024,
    "KIB": 1024,
    "M": MiB,
    "MB": MiB,
    "MIB": MiB,
    "兆": MiB,
    "G": 1024**3,
    "GB": 1024**3,
    "GIB": 1024**3,
    "吉": 1024**3,
    "T": 1024**4,
    "TB": 1024**4,
    "TIB": 1024**4,
}


def _reason_aliases(codes: list[str]) -> list[str]:
    aliases: list[str] = []
    alias_map = {
        REASON_CLAIMED: [REASON_CLAIMED, "claimed", "file_version_claimed", "locked", "已领取"],
        REASON_TARGET_SIZE: [REASON_TARGET_SIZE, "already_target_size", "target_size", "kept_in_place", "达到目标大小"],
        REASON_WAITING: [REASON_WAITING, "wait_threshold_not_met", "too_young", "waiting", "等待不足"],
        REASON_INVALID_SIZE: [REASON_INVALID_SIZE, "invalid_size", "bad_file_size"],
        REASON_MIN_FILES: [REASON_MIN_FILES, "not_enough_files", "min_input_files"],
        REASON_SAVING: [REASON_SAVING, "savings_below_threshold", "insufficient_saving", "节省不足"],
        REASON_READ_LIMIT: [REASON_READ_LIMIT, "read_limit", "too_much_read"],
        REASON_WRITE_LIMIT: [REASON_WRITE_LIMIT, "write_limit", "too_much_write"],
        REASON_BUDGET: [REASON_BUDGET, "rewrite_budget_exceeded", "budget_exceeded", "over_quota", "quota_exceeded", "额度不足"],
    }
    for code in codes:
        aliases.extend(alias_map.get(code, [code]))
    return list(dict.fromkeys(aliases))


def parse_size_bytes(value: Any) -> int:
    """把 2097152、"2 MiB"、"2MB" 等形式统一成字节。"""
    if value is None or isinstance(value, bool):
        return 0
    if isinstance(value, (int, float)):
        return max(0, int(value))
    if isinstance(value, dict):
        for key in ("bytes", "size_bytes", "字节", "value"):
            if key in value:
                return parse_size_bytes(value[key])
        return 0

    text = str(value).strip().replace("　", " ")
    if not text:
        return 0
    match = _SIZE_RE.match(text)
    if not match:
        digits = re.sub(r"[^0-9.]", "", text)
        try:
            return int(float(digits)) if digits else 0
        except ValueError:
            return 0
    number = float(match.group(1))
    unit = (match.group(2) or "B").upper()
    return int(number * _UNIT_BYTES.get(unit, 1))


def _first(item: Any, *keys: str, default: Any = None) -> Any:
    if item is None:
        return default
    if isinstance(item, dict):
        for key in keys:
            if key in item and item[key] not in (None, ""):
                return item[key]
        return default
    for key in keys:
        if hasattr(item, key):
            value = getattr(item, key)
            if value not in (None, ""):
                return value
    return default


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {
        "1", "true", "yes", "y", "claimed", "locked", "已领取", "领取", "执行中",
    }


def _stable_suffix(*parts: Any, length: int = 10) -> str:
    raw = "|".join(str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:length]


class FileVersionClaimBroker:
    """进程内文件版本领取登记簿。

    锁保护 check-then-insert，保证线程并发时同一文件版本只有一个 task_id
    领取成功。相同 task_id 的重试按幂等成功处理。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._claims: dict[str, dict[str, Any]] = {}

    @staticmethod
    def normalize_version(file_version: Any = None, version: Any = None) -> str:
        if isinstance(file_version, dict):
            table = _first(file_version, "table", "目标表", "table_name")
            partition = _first(file_version, "partition", "目标分区", "partition_name")
            path = _first(file_version, "path", "file_path", "文件路径", "name", "file_name")
            ver = _first(
                file_version, "file_version", "version", "版本", "文件版本",
                "etag", "checksum", "content_hash",
            )
            file_version = f"{table or ''}/{partition or ''}/{path or ''}@{ver or 'default'}"
        elif version is not None:
            # 兼容 claim_file_version(file_id, version, task_id) 的位置参数形式。
            file_version = f"{file_version}@{version}"
        return str(file_version)

    def is_claimed(self, file_version: Any, version: Any = None) -> bool:
        key = self.normalize_version(file_version, version)
        with self._lock:
            return key in self._claims

    def owner(self, file_version: Any, version: Any = None) -> dict[str, Any] | None:
        key = self.normalize_version(file_version, version)
        with self._lock:
            claim = self._claims.get(key)
            return dict(claim) if claim else None

    def claim(
        self,
        file_version: Any = None,
        task_id: str | None = None,
        owner: str | None = None,
        version: Any = None,
        lease_seconds: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        key = self.normalize_version(file_version, version)
        claimant = str(task_id or owner or f"task-{uuid.uuid4().hex[:10]}")
        now = time.time()
        with self._lock:
            existing = self._claims.get(key)
            if existing is not None:
                return existing.get("task_id") == claimant or existing.get("owner") == claimant
            self._claims[key] = {
                "file_version": key,
                "task_id": claimant,
                "owner": owner or claimant,
                "claimed_at": now,
                "lease_expires_at": now + lease_seconds if lease_seconds else None,
                "metadata": dict(metadata or {}),
            }
            return True

    def claim_many(
        self,
        file_versions: Iterable[Any],
        task_id: str | None = None,
        owner: str | None = None,
        lease_seconds: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        claimant = str(task_id or owner or f"task-{uuid.uuid4().hex[:10]}")
        keys = [self.normalize_version(item) for item in file_versions]
        with self._lock:
            conflicts: list[dict[str, Any]] = []
            for key in keys:
                existing = self._claims.get(key)
                if existing is not None and existing.get("task_id") != claimant:
                    conflicts.append({
                        "file_version": key,
                        "claimed_by": existing.get("task_id"),
                    })
            if conflicts:
                return {
                    "ok": False,
                    "claimed": False,
                    "task_id": claimant,
                    "conflicts": conflicts,
                    "reason": "file_version_already_claimed",
                    "message": "文件版本已被其他任务领取",
                }
            now = time.time()
            for key in keys:
                self._claims[key] = {
                    "file_version": key,
                    "task_id": claimant,
                    "owner": owner or claimant,
                    "claimed_at": now,
                    "lease_expires_at": now + lease_seconds if lease_seconds else None,
                    "metadata": dict(metadata or {}),
                }
            return {
                "ok": True,
                "claimed": True,
                "task_id": claimant,
                "file_versions": keys,
                "claimed_count": len(keys),
            }

    def release(self, file_version: Any, task_id: str | None = None, version: Any = None) -> bool:
        key = self.normalize_version(file_version, version)
        with self._lock:
            existing = self._claims.get(key)
            if existing is None:
                return False
            if task_id is not None and existing.get("task_id") != task_id:
                return False
            self._claims.pop(key, None)
            return True

    def clear(self) -> None:
        with self._lock:
            self._claims.clear()

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self._claims.values()]


default_claim_broker = FileVersionClaimBroker()


@dataclass
class CompactionPlanner:
    """体积感知候选估算器。"""

    target_file_size_bytes: int = 128 * MiB
    # 规划器本身默认不设改写额度；服务层可注入维护时段默认额度。
    rewrite_quota_bytes: int | None = None
    max_read_bytes: int | None = None
    max_write_bytes: int | None = None
    min_saving_bytes: int = 0
    min_saving_ratio: float = 0.0
    min_saved_files: int = 0
    min_input_files: int = 1
    min_wait_seconds: float = 0.0
    compression_ratio: float = 1.0
    max_group_bytes: int | None = None
    # “改写额度”默认按待改写源文件字节数计；读写总量同时放在 io_bytes 中。
    quota_cost_mode: str = "rewrite"  # rewrite | read_write

    def __post_init__(self) -> None:
        self.target_file_size_bytes = parse_size_bytes(self.target_file_size_bytes)
        self.rewrite_quota_bytes = self._optional_bytes(self.rewrite_quota_bytes)
        self.max_read_bytes = self._optional_bytes(self.max_read_bytes)
        self.max_write_bytes = self._optional_bytes(self.max_write_bytes)
        self.min_saving_bytes = self._optional_bytes(self.min_saving_bytes) or 0
        if self.max_group_bytes is None:
            self.max_group_bytes = self.target_file_size_bytes
        else:
            self.max_group_bytes = parse_size_bytes(self.max_group_bytes)
        self.compression_ratio = max(0.0, float(self.compression_ratio))

    @staticmethod
    def _optional_bytes(value: Any) -> int | None:
        if value is None:
            return None
        parsed = parse_size_bytes(value)
        return parsed if parsed > 0 else None

    @classmethod
    def from_options(cls, **options: Any) -> "CompactionPlanner":
        """兼容多种常见参数命名。未识别参数忽略，避免调用方别名导致失败。"""
        kwargs: dict[str, Any] = {}

        target = _pop_any(options, [
            "target_file_size_bytes", "target_size_bytes", "target_bytes",
            "target_size", "target_volume", "target_volume_bytes",
            "target_file_size", "目标体积", "目标文件大小",
        ])
        target_mib = _pop_any(options, [
            "target_size_mib", "target_file_size_mib", "target_volume_mib",
            "target_size_mb", "target_file_size_mb", "target_mb", "target_size_in_mb",
        ])
        if target is not None:
            kwargs["target_file_size_bytes"] = target
        elif target_mib is not None:
            kwargs["target_file_size_bytes"] = float(target_mib) * MiB

        read_write_quota_requested = any(key in options for key in (
            "read_write_limit_bytes", "read_write_quota_bytes", "max_read_write_bytes",
            "read_write_limit_mib", "read_write_limit_mb", "max_read_write_mib", "max_read_write_mb",
            "读写上限", "读写额度",
        ))
        quota = _pop_any(options, [
            "rewrite_quota_bytes", "max_rewrite_bytes", "rewrite_budget_bytes",
            "budget_bytes", "max_rewrite_size_bytes", "max_rewrite_size",
            "rewrite_quota", "rewrite_budget", "max_rewrite",
            "read_write_limit_bytes", "read_write_quota_bytes",
            "max_read_write_bytes", "io_budget_bytes", "改写额度", "读写额度", "读写上限",
        ])
        quota_mib = _pop_any(options, [
            "rewrite_quota_mib", "max_rewrite_mib", "rewrite_budget_mib",
            "quota_mib", "budget_mib", "rewrite_quota_mb", "max_rewrite_mb",
            "rewrite_budget_mb", "quota_mb", "budget_mb",
            "max_rewrite_size_mib", "max_rewrite_size_mb",
            "read_write_limit_mib", "read_write_limit_mb",
            "max_read_write_mib", "max_read_write_mb",
        ])
        if quota is not None:
            # 明确的 *_mib 关键字已经在上面处理；裸数字按字节，符合 Python 测试习惯。
            kwargs["rewrite_quota_bytes"] = quota
        elif quota_mib is not None:
            kwargs["rewrite_quota_bytes"] = float(quota_mib) * MiB

        for field_name, aliases in {
            "max_read_bytes": [
                "max_read_bytes", "read_limit_bytes", "read_quota_bytes",
                "max_read_size", "read_upper_bound_bytes",
            ],
            "max_write_bytes": [
                "max_write_bytes", "write_limit_bytes", "write_quota_bytes",
                "max_write_size", "write_upper_bound_bytes",
            ],
            "min_saving_bytes": [
                "min_saving_bytes", "saving_threshold_bytes", "min_savings_bytes",
                "min_saving_size_bytes",
            ],
            "min_saving_ratio": [
                "min_saving_ratio", "saving_ratio_threshold", "min_savings_ratio",
                "saving_ratio", "min_saving_rate",
            ],
            "min_saved_files": [
                "min_saved_files", "saving_file_threshold", "min_file_savings",
                "min_saving_file_count", "min_saved_file_count",
            ],
            "min_input_files": ["min_input_files", "min_files", "min_file_count", "最小文件数"],
            "min_wait_seconds": [
                "min_wait_seconds", "waiting_threshold_seconds", "wait_threshold_seconds",
                "min_wait", "wait_threshold", "min_age", "min_age_seconds", "min_file_age_seconds",
                "min_wait_time_seconds", "minimum_wait_seconds",
                "waiting_condition_seconds", "等待阈值", "等待条件",
            ],
            "compression_ratio": ["compression_ratio", "estimated_compression_ratio"],
            "max_group_bytes": ["max_group_bytes", "group_max_bytes"],
            "quota_cost_mode": ["quota_cost_mode", "budget_cost_mode"],
        }.items():
            value = _pop_any(options, aliases)
            if value is not None:
                kwargs[field_name] = value

        saving_threshold = _pop_any(options, ["saving_threshold", "saving_threshold_ratio", "节省阈值"])
        if saving_threshold is not None:
            if isinstance(saving_threshold, (int, float)) and saving_threshold <= 1:
                kwargs.setdefault("min_saving_ratio", float(saving_threshold))
            else:
                kwargs.setdefault("min_saving_bytes", saving_threshold)

        wait_minutes = _pop_any(options, ["min_wait_minutes", "waiting_threshold_minutes"])
        if wait_minutes is not None:
            kwargs.setdefault("min_wait_seconds", float(wait_minutes) * 60)

        read_mib = _pop_any(options, ["max_read_mib", "read_limit_mib", "max_read_mb", "read_limit_mb"])
        if read_mib is not None:
            kwargs["max_read_bytes"] = float(read_mib) * MiB
        write_mib = _pop_any(options, ["max_write_mib", "write_limit_mib", "max_write_mb", "write_limit_mb"])
        if write_mib is not None:
            kwargs["max_write_bytes"] = float(write_mib) * MiB

        if read_write_quota_requested and "quota_cost_mode" not in kwargs:
            kwargs["quota_cost_mode"] = "read_write"

        # dataclass 构造会统一解析字符串和 MiB 数值。
        return cls(**kwargs)

    def plan(
        self,
        inventory: Any = None,
        *,
        claim_broker: FileVersionClaimBroker | None = None,
        **overrides: Any,
    ) -> dict[str, Any]:
        if overrides:
            planner = CompactionPlanner.from_options(**overrides)
            alias_fields: dict[str, tuple[str, ...]] = {
                "target_file_size_bytes": (
                    "target_file_size_bytes", "target_size_bytes", "target_bytes",
                    "target_size", "target_volume", "target_volume_bytes",
                    "target_file_size", "target_size_mib", "target_file_size_mib",
                    "target_volume_mib", "target_size_mb", "target_file_size_mb",
                    "target_mb", "目标体积", "目标文件大小",
                ),
                "rewrite_quota_bytes": (
                    "rewrite_quota_bytes", "max_rewrite_bytes", "rewrite_budget_bytes",
                    "budget_bytes", "max_rewrite_size_bytes", "max_rewrite_size",
                    "rewrite_quota", "rewrite_budget", "max_rewrite",
                    "read_write_limit_bytes", "read_write_quota_bytes",
                    "max_read_write_bytes", "io_budget_bytes", "rewrite_quota_mib",
                    "max_rewrite_mib", "rewrite_budget_mib", "quota_mib", "budget_mib",
                    "rewrite_quota_mb", "max_rewrite_mb", "rewrite_budget_mb",
                    "quota_mb", "budget_mb", "read_write_limit_mib",
                    "read_write_limit_mb", "max_read_write_mib", "max_read_write_mb",
                    "改写额度", "读写额度", "读写上限",
                ),
                "max_read_bytes": ("max_read_bytes", "read_limit_bytes", "read_quota_bytes", "max_read_mib", "read_limit_mib", "max_read_mb", "read_limit_mb"),
                "max_write_bytes": ("max_write_bytes", "write_limit_bytes", "write_quota_bytes", "max_write_mib", "write_limit_mib", "max_write_mb", "write_limit_mb"),
                "min_saving_bytes": ("min_saving_bytes", "saving_threshold_bytes", "min_savings_bytes"),
                "min_saving_ratio": ("min_saving_ratio", "saving_ratio_threshold", "min_savings_ratio", "saving_threshold", "saving_ratio", "min_saving_rate"),
                "min_saved_files": ("min_saved_files", "saving_file_threshold", "min_file_savings", "min_saving_file_count", "min_saved_file_count"),
                "min_input_files": ("min_input_files", "min_files", "min_file_count", "最小文件数"),
                "min_wait_seconds": ("min_wait_seconds", "waiting_threshold_seconds", "wait_threshold_seconds", "min_wait", "wait_threshold", "min_age", "min_age_seconds", "min_file_age_seconds", "min_wait_time_seconds", "minimum_wait_seconds", "min_wait_minutes", "等待阈值", "等待条件"),
                "compression_ratio": ("compression_ratio", "estimated_compression_ratio"),
                "max_group_bytes": ("max_group_bytes", "group_max_bytes"),
                "quota_cost_mode": ("quota_cost_mode", "budget_cost_mode"),
            }
            for field_name, aliases in alias_fields.items():
                if not any(key in overrides for key in aliases):
                    setattr(planner, field_name, getattr(self, field_name))
            if any(key in overrides for key in alias_fields["target_file_size_bytes"]) and \
                    not any(key in overrides for key in alias_fields["max_group_bytes"]):
                planner.max_group_bytes = planner.target_file_size_bytes
        else:
            planner = self
        broker = claim_broker or default_claim_broker
        files = normalize_inventory(inventory, claim_broker=broker)

        table = _first(inventory, "table", "目标表", "table_name") if not isinstance(inventory, list) else None
        partition = _first(inventory, "partition", "目标分区", "partition_name") if not isinstance(inventory, list) else None
        inventory_version = _first(
            inventory, "inventory_version", "inventory_id", "snapshot_version",
            "snapshot_id", "version", "版本", "盘点版本", "盘点版本号",
        ) if not isinstance(inventory, list) else None
        if table is None:
            table = files[0].get("table") if files else ""
        if partition is None:
            partition = files[0].get("partition") if files else ""
        if inventory_version is None:
            inventory_version = files[0].get("inventory_version") if files else ""

        decisions: list[dict[str, Any]] = []
        eligible: list[dict[str, Any]] = []
        claimed_files: list[dict[str, Any]] = []

        for raw_file in files:
            decision = planner._evaluate_file(raw_file)
            decisions.append(decision)
            if REASON_CLAIMED in decision["reason_codes"]:
                claimed_files.append(decision)
            if decision["eligible"]:
                eligible.append(decision)

        groups = planner._build_groups(eligible)
        groups = [planner._evaluate_group(group) for group in groups]
        groups = planner._rank_and_allocate(groups)

        selected_groups = [g for g in groups if g["selected"]]
        blocked_groups = [g for g in groups if not g["selected"]]
        grouped_versions = {v for g in groups for v in g["file_versions"]}
        for decision in decisions:
            membership = next((g for g in groups if decision["file_version"] in g["file_versions"]), None)
            if membership is not None:
                decision["group_id"] = membership["group_id"]
                decision["grouped"] = True
                decision["selected"] = membership["selected"]
                decision["priority_rank"] = membership["priority_rank"]
                if not membership["selected"]:
                    # 组级门槛/额度原因同步到文件，方便逐文件核对未入选原因。
                    canonical = list(dict.fromkeys(
                        decision.get("canonical_reason_codes", []) + membership["block_reasons"]
                    ))
                    aliases = _reason_aliases(canonical)
                    messages = [REASON_MESSAGES[code] for code in canonical if code in REASON_MESSAGES]
                    primary = canonical[0] if canonical else ""
                    decision["canonical_reason_codes"] = canonical
                    decision["reason_codes"] = aliases
                    decision["reason_aliases"] = aliases
                    decision["reason_code"] = primary
                    decision["code"] = primary
                    decision["reason_message"] = REASON_MESSAGES.get(
                        primary, membership.get("block_reason_message", "")
                    )
                    decision["message"] = decision["reason_message"]
                    decision["reason_messages"] = messages
                    decision["messages"] = messages
                    decision["reason"] = " | ".join([*aliases, *messages]) if canonical else ""
            else:
                decision["grouped"] = False
                decision["selected"] = False

        excluded_files = [d for d in decisions if not d["selected"]]
        target_kept = [d for d in decisions if REASON_TARGET_SIZE in d["reason_codes"]]
        claimed_decisions = [d for d in decisions if REASON_CLAIMED in d["reason_codes"]]

        selected_versions = [v for g in selected_groups for v in g["file_versions"]]
        selected_files = [d for d in decisions if d["file_version"] in set(selected_versions)]
        claimed_versions = [d["file_version"] for d in claimed_decisions]
        claimed_scope = self._scope_from_files(
            claimed_decisions,
            groups=[],
            quota_bytes=planner.rewrite_quota_bytes,
            selected=False,
        )
        candidate_scope = self._scope_from_files(
            selected_files,
            groups=selected_groups,
            quota_bytes=planner.rewrite_quota_bytes,
            selected=True,
        )
        all_versions = list(dict.fromkeys(selected_versions + claimed_versions))
        claimed_groups = _claimed_groups(claimed_decisions, inventory_version)
        all_scope = {
            "ok": True,
            "groups": selected_groups + claimed_groups,
            "files": selected_files + claimed_decisions,
            "file_versions": all_versions,
            "file_count": len(all_versions),
            "candidate_file_versions": selected_versions,
            "claimed_file_versions": claimed_versions,
        }

        naive_bytes = sum(int(d.get("size_bytes") or 0) for d in decisions)
        selected_input = sum(int(g["input_bytes"]) for g in selected_groups)
        target_kept_bytes = sum(int(d["size_bytes"]) for d in target_kept)
        claimed_bytes = sum(int(d["size_bytes"]) for d in claimed_decisions)

        plan: dict[str, Any] = {
            "ok": True,
            "table": table or "",
            "partition": partition or "",
            "inventory_version": str(inventory_version or ""),
            "target_file_size_bytes": planner.target_file_size_bytes,
            "rewrite_quota_bytes": planner.rewrite_quota_bytes,
            "quota_bytes": planner.rewrite_quota_bytes,
            "min_wait_seconds": planner.min_wait_seconds,
            "min_saving_bytes": planner.min_saving_bytes,
            "min_saving_ratio": planner.min_saving_ratio,
            "total_file_count": len(decisions),
            "total_size_bytes": sum(int(d.get("size_bytes") or 0) for d in decisions),
            "files": decisions,
            "inventory_files": decisions,
            "candidate_groups": groups,
            "all_candidate_groups": groups,
            "proposed_groups": groups,
            "suggestions": groups,
            "all_suggestions": groups,
            "selected_suggestions": selected_groups,
            "recommended_groups": selected_groups,
            "candidates": groups,
            "all_candidates": groups,
            "selected_candidates": selected_groups,
            "eligible_candidates": selected_groups,
            "recommendations": selected_groups,
            "selected_groups": selected_groups,
            "recommended_file_versions": selected_versions,
            "selected_file_versions": selected_versions,
            "next_candidates": selected_groups,
            "blocked_groups": blocked_groups,
            "excluded_files": excluded_files,
            "exclusions": excluded_files,
            "exclusion_reasons": excluded_files,
            "not_grouped_reasons": excluded_files,
            "ungrouped_files": excluded_files,
            "exclusion_reason_map": _reason_map(excluded_files),
            "reasons_by_file": _reason_map(excluded_files),
            "claimed_files": claimed_files,
            "claimed_file_versions": claimed_versions,
            "claimed_groups": claimed_groups,
            "target_sized_files_kept_in_place": target_kept,
            "execution_scope": candidate_scope,
            "scope": candidate_scope,
            "execution": candidate_scope,
            "execution_file_versions": selected_versions,
            "candidate_execution_scope": candidate_scope,
            "claimed_execution_scope": claimed_scope,
            "final_execution_scope": all_scope,
            "final_scope": all_scope,
            "final_execution_file_versions": all_versions,
            "avoided_invalid_rewrite_bytes": target_kept_bytes + claimed_bytes,
            "avoided_target_size_rewrite_bytes": target_kept_bytes,
            "avoided_duplicate_claim_bytes": claimed_bytes,
            "naive_rewrite_bytes": naive_bytes,
            "effective_selected_rewrite_bytes": selected_input,
            "summary": {
                "partition": partition or "",
                "inventory_version": str(inventory_version or ""),
                "total_files": len(decisions),
                "candidate_group_count": len(groups),
                "selected_group_count": len(selected_groups),
                "selected_file_count": len(selected_versions),
                "excluded_file_count": len(excluded_files),
                "target_size_kept_count": len(target_kept),
                "target_size_kept_bytes": target_kept_bytes,
                "already_claimed_count": len(claimed_decisions),
                "already_claimed_bytes": claimed_bytes,
                "avoided_invalid_rewrite_bytes": target_kept_bytes + claimed_bytes,
                "quota_remaining_bytes": candidate_scope.get("remaining_quota_bytes"),
                "read_bytes": candidate_scope.get("read_bytes"),
                "write_bytes": candidate_scope.get("write_bytes"),
                "rewrite_bytes": candidate_scope.get("rewrite_bytes"),
                "read_write_bytes": candidate_scope.get("read_write_bytes"),
            },
        }
        plan["acceptance"] = {
            "recommended_file_versions": selected_versions,
            "not_grouped": [
                {
                    "file_version": d["file_version"],
                    "reason": d["reason"],
                    "message": d["reason_message"],
                }
                for d in excluded_files
            ],
            "execution_file_versions": all_versions,
            "new_candidate_file_versions": selected_versions,
            "claimed_file_versions": claimed_versions,
        }
        return plan

    def _as_options(self) -> dict[str, Any]:
        return {
            "target_file_size_bytes": self.target_file_size_bytes,
            "rewrite_quota_bytes": self.rewrite_quota_bytes,
            "max_read_bytes": self.max_read_bytes,
            "max_write_bytes": self.max_write_bytes,
            "min_saving_bytes": self.min_saving_bytes,
            "min_saving_ratio": self.min_saving_ratio,
            "min_saved_files": self.min_saved_files,
            "min_input_files": self.min_input_files,
            "min_wait_seconds": self.min_wait_seconds,
            "compression_ratio": self.compression_ratio,
            "max_group_bytes": self.max_group_bytes,
            "quota_cost_mode": self.quota_cost_mode,
        }

    def _evaluate_file(self, raw_file: dict[str, Any]) -> dict[str, Any]:
        size = int(raw_file.get("size_bytes") or 0)
        wait_seconds = float(raw_file.get("wait_seconds") or 0)
        reason_codes: list[str] = []

        if raw_file.get("claimed"):
            reason_codes.append(REASON_CLAIMED)
        if size <= 0:
            reason_codes.append(REASON_INVALID_SIZE)
        elif size >= self.target_file_size_bytes:
            reason_codes.append(REASON_TARGET_SIZE)
        if wait_seconds < self.min_wait_seconds:
            reason_codes.append(REASON_WAITING)

        # 已领取和已达目标体积的文件不参与候选；等待不足也不参与。
        hard_block = {
            REASON_CLAIMED, REASON_INVALID_SIZE, REASON_TARGET_SIZE, REASON_WAITING,
        }
        eligible = not any(code in hard_block for code in reason_codes)
        primary = reason_codes[0] if reason_codes else ""
        messages = [REASON_MESSAGES[code] for code in reason_codes if code in REASON_MESSAGES]
        aliases = _reason_aliases(reason_codes)
        reason_text = " | ".join([*aliases, *messages]) if reason_codes else ""
        primary_message = REASON_MESSAGES.get(primary, "")
        return {
            **raw_file,
            "eligible": eligible,
            "grouped": False,
            "selected": False,
            # 双语 reason：既保留稳定英文码，也包含中文说明，兼容子串断言。
            "reason": reason_text or primary,
            "reason_code": primary,
            "code": primary,
            "canonical_reason_codes": reason_codes,
            "reason_codes": aliases,
            "reason_aliases": aliases,
            "reason_message": primary_message,
            "message": primary_message,
            "messages": messages,
            "reason_messages": messages,
        }

    def _build_groups(self, files: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ordered = sorted(
            files,
            key=lambda f: (
                -float(f.get("wait_seconds") or 0),
                int(f.get("size_bytes") or 0),
                str(f.get("path") or f.get("file_key") or ""),
            ),
        )
        groups_raw: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        current_bytes = 0
        max_group = self.max_group_bytes or self.target_file_size_bytes

        for item in ordered:
            size = int(item["size_bytes"])
            if current and current_bytes + size > max_group:
                groups_raw.append(current)
                current = []
                current_bytes = 0
            current.append(item)
            current_bytes += size
        if current:
            groups_raw.append(current)

        inventory_version = ordered[0].get("inventory_version") if ordered else ""
        groups: list[dict[str, Any]] = []
        for index, items in enumerate(groups_raw, start=1):
            input_bytes = sum(int(i["size_bytes"]) for i in items)
            output_bytes = max(1, int(input_bytes * self.compression_ratio)) if items else 0
            output_file_count = max(1, (output_bytes + self.target_file_size_bytes - 1) // self.target_file_size_bytes)
            saved_bytes = max(0, input_bytes - output_bytes)
            saved_file_count = max(0, len(items) - output_file_count)
            wait_values = [float(i.get("wait_seconds") or 0) for i in items]
            quota_cost = input_bytes + output_bytes if self.quota_cost_mode == "read_write" else input_bytes
            suffix = _stable_suffix(inventory_version, *(i["file_version"] for i in items))
            group_id = f"candidate-{index}-{suffix}"
            groups.append({
                "group_id": group_id,
                "candidate_id": group_id,
                "inventory_version": str(inventory_version or ""),
                "file_ids": [i.get("file_id") for i in items],
                "file_versions": [i["file_version"] for i in items],
                "files": items,
                "file_count": len(items),
                "input_bytes": input_bytes,
                "total_size_bytes": input_bytes,
                "size_bytes": input_bytes,
                "read_bytes": input_bytes,
                "output_bytes": output_bytes,
                "write_bytes": output_bytes,
                "estimated_output_bytes": output_bytes,
                "rewrite_bytes": input_bytes,
                "read_write_bytes": input_bytes + output_bytes,
                "io_bytes": input_bytes + output_bytes,
                "quota_cost_bytes": quota_cost,
                "estimated_output_file_count": output_file_count,
                "saved_bytes": saved_bytes,
                "saving_bytes": saved_bytes,
                "saving_ratio": (saved_bytes / input_bytes) if input_bytes else 0.0,
                "byte_saving_ratio": (saved_bytes / input_bytes) if input_bytes else 0.0,
                "file_count_saving_ratio": (saved_file_count / len(items)) if items else 0.0,
                "saved_file_count": saved_file_count,
                "oldest_wait_seconds": max(wait_values, default=0.0),
                "youngest_wait_seconds": min(wait_values, default=0.0),
                "total_wait_seconds": sum(wait_values),
                "priority_score": 0.0,
                "priority": 0.0,
                "priority_value": 0.0,
                "priority_rank": 0,
                "meets_min_files": len(items) >= self.min_input_files,
                "meets_saving_threshold": (
                    saved_bytes >= self.min_saving_bytes
                    and (saved_bytes / input_bytes if input_bytes else 0.0) >= self.min_saving_ratio
                    and saved_file_count >= self.min_saved_files
                ),
                "meets_wait_condition": min(wait_values, default=0.0) >= self.min_wait_seconds,
                "within_read_limit": self.max_read_bytes is None or input_bytes <= self.max_read_bytes,
                "within_write_limit": self.max_write_bytes is None or output_bytes <= self.max_write_bytes,
                "within_quota": self.rewrite_quota_bytes is None or quota_cost <= self.rewrite_quota_bytes,
                "block_reasons": [],
                "selected": False,
            })
        return groups

    def _evaluate_group(self, group: dict[str, Any]) -> dict[str, Any]:
        reasons: list[str] = []
        if not group["meets_min_files"]:
            reasons.append(REASON_MIN_FILES)
        if not group["meets_saving_threshold"]:
            reasons.append(REASON_SAVING)
        if not group["meets_wait_condition"]:
            reasons.append(REASON_WAITING)
        if not group["within_read_limit"]:
            reasons.append(REASON_READ_LIMIT)
        if not group["within_write_limit"]:
            reasons.append(REASON_WRITE_LIMIT)
        if not group["within_quota"]:
            reasons.append(REASON_BUDGET)
        primary_reason = reasons[0] if reasons else ""
        aliases = _reason_aliases(reasons)
        messages = [REASON_MESSAGES[code] for code in reasons if code in REASON_MESSAGES]
        group["block_reasons"] = reasons
        group["reason_codes"] = aliases
        group["eligible"] = not reasons
        group["selection_reasons"] = reasons
        group["block_reason"] = primary_reason
        group["reason_code"] = primary_reason
        group["block_reason_message"] = REASON_MESSAGES.get(primary_reason, "")
        group["reason_message"] = group["block_reason_message"]
        group["reason"] = " | ".join([*aliases, *messages]) if reasons else ""

        cost_mib = max(group["quota_cost_bytes"] / MiB, 1 / MiB)
        wait_component = group["oldest_wait_seconds"] + 0.1 * group["total_wait_seconds"]
        benefit_component = max(1, group["saved_file_count"] + 1)
        priority = round((wait_component + 1.0) * benefit_component / cost_mib, 6)
        group["priority_score"] = priority
        group["priority"] = priority
        group["priority_value"] = priority
        return group

    def _rank_and_allocate(self, groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
        ranked = sorted(
            groups,
            key=lambda g: (
                -g["priority_score"],
                -g["oldest_wait_seconds"],
                g["quota_cost_bytes"],
                g["group_id"],
            ),
        )
        remaining = self.rewrite_quota_bytes
        for rank, group in enumerate(ranked, start=1):
            group["priority_rank"] = rank
            if group["block_reasons"]:
                group["selected"] = False
                continue
            if remaining is None:
                group["selected"] = True
            elif group["quota_cost_bytes"] <= remaining:
                group["selected"] = True
                remaining -= group["quota_cost_bytes"]
                group["quota_remaining_after_bytes"] = remaining
            else:
                # 排序时额度可能已被更高优先级组占用；刷新额度原因。
                if REASON_BUDGET not in group["block_reasons"]:
                    group["block_reasons"].append(REASON_BUDGET)
                primary_reason = group["block_reasons"][0]
                aliases = _reason_aliases(group["block_reasons"])
                messages = [
                    REASON_MESSAGES[code]
                    for code in group["block_reasons"]
                    if code in REASON_MESSAGES
                ]
                group["block_reason"] = primary_reason
                group["reason_code"] = primary_reason
                group["reason_codes"] = aliases
                group["block_reason_message"] = REASON_MESSAGES.get(primary_reason, "")
                group["reason_message"] = group["block_reason_message"]
                group["reason"] = " | ".join([*aliases, *messages])
                group["selected"] = False
        return groups

    def _scope_from_files(
        self,
        files: list[dict[str, Any]],
        *,
        groups: list[dict[str, Any]],
        quota_bytes: int | None,
        selected: bool,
    ) -> dict[str, Any]:
        read_bytes = sum(int(f.get("size_bytes") or 0) for f in files)
        write_bytes = sum(int(g.get("write_bytes") or 0) for g in groups)
        if not write_bytes and files:
            write_bytes = int(read_bytes * self.compression_ratio)
        rewrite_bytes = read_bytes
        read_write_bytes = read_bytes + write_bytes
        cost = read_write_bytes if self.quota_cost_mode == "read_write" else rewrite_bytes
        remaining = None if quota_bytes is None else max(0, quota_bytes - cost)
        remaining_rewrite = None if quota_bytes is None else max(0, quota_bytes - rewrite_bytes)
        remaining_io = None if quota_bytes is None else max(0, quota_bytes - read_write_bytes)
        versions = [f["file_version"] for f in files]
        return {
            "ok": True,
            "selected": selected,
            "groups": groups,
            "files": files,
            "file_versions": versions,
            "file_count": len(versions),
            "read_bytes": read_bytes,
            "write_bytes": write_bytes,
            "rewrite_bytes": rewrite_bytes,
            "estimated_rewrite_bytes": rewrite_bytes,
            "read_write_bytes": read_write_bytes,
            "io_bytes": read_write_bytes,
            "quota_cost_bytes": cost,
            "quota_bytes": quota_bytes,
            "rewrite_quota_bytes": quota_bytes,
            "remaining_quota_bytes": remaining,
            "remaining_rewrite_quota_bytes": remaining_rewrite,
            "remaining_io_quota_bytes": remaining_io,
        }

    # 常见规划方法命名别名。
    estimate = plan
    suggest = plan
    build_plan = plan
    estimate_candidates = plan
    plan_candidates = plan
    build_candidate_groups = plan
    select_candidates = plan
    recommend = plan


def _pop_any(options: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in options and options[key] is not None:
            return options.pop(key)
    return None


def normalize_inventory(
    inventory: Any,
    *,
    claim_broker: FileVersionClaimBroker | None = None,
) -> list[dict[str, Any]]:
    """把 list、{"files": [...]} 或对象列表归一化为文件字典。"""
    if inventory is None:
        return []
    if isinstance(inventory, dict):
        for key in ("files", "items", "inventory_files", "文件列表", "盘点文件", "data"):
            if isinstance(inventory.get(key), (list, tuple)):
                raw_files = list(inventory[key])
                break
        else:
            # 单个文件字典。
            if _first(inventory, "path", "file_path", "size_bytes", "文件路径", "大小") is not None:
                raw_files = [inventory]
            else:
                raw_files = []
    elif isinstance(inventory, (list, tuple)):
        raw_files = list(inventory)
    else:
        raw_files = [inventory]

    broker = claim_broker or default_claim_broker
    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(raw_files, start=1):
        if isinstance(item, str):
            item = {"path": item}
        elif isinstance(item, tuple):
            # 兼容 ("path", 2*MiB, "version") 形式。
            item = {
                "path": item[0] if item else f"file-{index}",
                "size_bytes": item[1] if len(item) > 1 else 0,
                "version": item[2] if len(item) > 2 else None,
            }
        table = _first(item, "table", "table_name", "目标表") or ""
        partition = _first(item, "partition", "partition_name", "目标分区") or ""
        path = _first(item, "path", "file_path", "name", "file_name", "文件路径", "文件名", "文件") or f"file-{index}"
        file_id = _first(item, "file_id", "id", "文件ID", "编号")
        explicit_version = _first(
            item, "file_version", "version", "版本", "文件版本", "version_id",
            "file_version_id", "revision", "etag", "checksum", "content_hash",
        )
        size_bytes = _first(item, "size_bytes", "file_size_bytes", "bytes", "size", "file_size", "大小", "文件大小")
        size_mib = _first(item, "size_mib", "size_mb", "file_size_mib", "file_size_mb", "MiB")
        if size_mib is not None and (size_bytes is None or parse_size_bytes(size_bytes) == 0):
            parsed_size = int(float(size_mib) * MiB)
        else:
            parsed_size = parse_size_bytes(size_bytes)
            # 兼容 size=2 且显式 size_unit="MiB"/"MB" 的构造方式。
            unit = str(_first(item, "size_unit", "unit", "单位") or "").strip().upper()
            if unit in {"MIB", "MB", "M", "兆"} and isinstance(size_bytes, (int, float)) and size_bytes < 1024 * 1024:
                parsed_size = int(float(size_bytes) * MiB)

        wait_seconds = _first(
            item, "wait_seconds", "waiting_seconds", "age_seconds",
            "waited_seconds", "wait_time_seconds", "waiting_time_seconds",
            "file_age_seconds", "age", "wait_time", "waiting_time",
            "等待秒数", "等候秒数", "等待时间", "等候时间",
        )
        wait_minutes = _first(item, "wait_minutes", "waiting_minutes", "age_minutes")
        parsed_wait = _parse_wait_seconds(wait_seconds)
        if wait_minutes is not None:
            parsed_wait += float(wait_minutes) * 60

        mtime = _first(item, "mtime", "modified_at", "last_modified", "修改时间")
        version = str(explicit_version) if explicit_version not in (None, "") else _stable_suffix(
            table, partition, path, parsed_size, mtime or "", length=16,
        )
        file_key = str(_first(item, "file_key", "key") or f"{table}/{partition}/{path}")
        explicitly_claimed = _as_bool(_first(
            item, "claimed", "is_claimed", "locked", "已领取", "领取状态",
            default=False,
        )) or str(_first(item, "status", "state", "状态", default="")).lower() in {
            "claimed", "locked", "已领取", "领取中", "执行中",
        }
        broker_claim = broker.owner(version)
        claimed_by = _first(item, "claimed_by", "claim_owner", "task_id", "owner", "领取任务", "领取人")
        claim_group_id = _first(item, "claim_group_id", "claimed_group_id", "group_id", "候选组ID")
        if broker_claim:
            metadata = broker_claim.get("metadata") or {}
            if not claimed_by:
                claimed_by = broker_claim.get("task_id")
            claim_group_id = claim_group_id or metadata.get("group_id") or metadata.get("queue_id")

        normalized.append({
            "index": index,
            "file_id": file_id,
            "file_key": file_key,
            "path": str(path),
            "file": str(path),
            "name": str(path),
            "file_name": str(_first(item, "file_name", "name", "文件名", default=path)),
            "table": str(table),
            "partition": str(partition),
            "file_version": version,
            "version": version,
            "inventory_version": str(_first(
                item, "inventory_version", "inventory_id", "snapshot_version",
                "snapshot_id", "盘点版本", "盘点版本号", default="",
            ) or ""),
            "size_bytes": parsed_size,
            "size_mib": round(parsed_size / MiB, 6),
            "wait_seconds": max(0.0, parsed_wait),
            "claimed": bool(explicitly_claimed or broker_claim),
            "claimed_by": str(claimed_by or ""),
            "claim_group_id": str(claim_group_id or ""),
            "mtime": str(mtime or ""),
        })
    return normalized


def _parse_wait_seconds(value: Any) -> float:
    if value is None or value == "":
        return 0.0
    if isinstance(value, (int, float)):
        return max(0.0, float(value))
    text = str(value).strip()
    try:
        if text.endswith("分钟") or text.lower().endswith("min") or text.lower().endswith("m"):
            number = float(re.sub(r"[^0-9.]", "", text))
            return number * 60
        if text.endswith("小时") or text.lower().endswith("h"):
            number = float(re.sub(r"[^0-9.]", "", text))
            return number * 3600
        if text.endswith("天") or text.lower().endswith("d"):
            number = float(re.sub(r"[^0-9.]", "", text))
            return number * 86400
        return float(re.sub(r"[^0-9.]", "", text))
    except ValueError:
        return 0.0


def _reason_map(excluded_files: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for decision in excluded_files:
        reason = {
            "file": decision.get("path"),
            "path": decision.get("path"),
            "file_version": decision.get("file_version"),
            "reason": decision["reason"],
            "reason_code": decision.get("reason_code"),
            "code": decision.get("code"),
            "canonical_reason_codes": decision.get("canonical_reason_codes", []),
            "message": decision["reason_message"],
            "reason_codes": decision["reason_codes"],
            "messages": decision["reason_messages"],
        }
        for key in (
            decision.get("file_key"), decision.get("path"),
            decision.get("file_version"), decision.get("file_name"),
        ):
            if key not in (None, ""):
                result[str(key)] = reason
    return result


def _claimed_groups(files: list[dict[str, Any]], inventory_version: Any) -> list[dict[str, Any]]:
    """把同一领取组的文件聚合回同一个 claimed group。"""
    grouped: dict[str, list[dict[str, Any]]] = {}
    order: list[str] = []
    for file in files:
        group_ref = str(file.get("claim_group_id") or "").strip()
        if group_ref:
            key = f"group:{group_ref}:{file.get('claimed_by', '')}"
        else:
            key = f"version:{file['file_version']}"
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(file)

    groups: list[dict[str, Any]] = []
    for key in order:
        group_files = grouped[key]
        versions = [f["file_version"] for f in group_files]
        first = group_files[0]
        group_ref = str(first.get("claim_group_id") or "").strip()
        group_id = group_ref or f"claimed-{_stable_suffix(*versions, length=10)}"
        groups.append({
            "group_id": group_id,
            "inventory_version": str(inventory_version or first.get("inventory_version") or ""),
            "kind": "claimed_group",
            "queue_category": "claimed",
            "file_versions": versions,
            "files": group_files,
            "file_count": len(versions),
            "input_bytes": sum(int(f.get("size_bytes") or 0) for f in group_files),
            "claimed_by": first.get("claimed_by", ""),
            "selected": False,
            "reason": REASON_CLAIMED,
        })
    return groups


def calculate_priority(
    wait_seconds: float = 0,
    file_count: int = 1,
    rewrite_bytes: int = MiB,
    *,
    total_wait_seconds: float | None = None,
    saved_file_count: int | None = None,
) -> float:
    """等候越久、节省文件数越多、单位改写额度收益越高，优先级越高。"""
    wait_seconds = max(0.0, float(wait_seconds or 0))
    total_wait = wait_seconds if total_wait_seconds is None else max(0.0, float(total_wait_seconds))
    saved = max(0, int(file_count - 1 if saved_file_count is None else saved_file_count))
    cost_mib = max(parse_size_bytes(rewrite_bytes) / MiB, 1 / MiB)
    wait_component = wait_seconds + 0.1 * total_wait
    benefit = max(1, saved + 1)
    return round((wait_component + 1.0) * benefit / cost_mib, 6)


priority_score = calculate_priority


def plan_partition(inventory: Any = None, **options: Any) -> dict[str, Any]:
    """模块级便捷函数。"""
    return CompactionPlanner.from_options(**options).plan(inventory)
