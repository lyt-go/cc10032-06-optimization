"""兼容性别名模块：体积感知规划逻辑的常用命名之一。"""
from __future__ import annotations

from app.services.compaction_planner import *  # noqa: F401,F403
from app.services.compaction_planner import (  # noqa: F401
    MiB,
    CompactionPlanner,
    FileVersionClaimBroker,
    calculate_priority,
    default_claim_broker,
    normalize_inventory,
    parse_size_bytes,
    plan_partition,
)

CompactionPlanningService = CompactionPlanner
CompactionCandidatePlanner = CompactionPlanner
