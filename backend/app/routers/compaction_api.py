"""压缩规划/领取的专用 HTTP 接口。

通用 CRUD 和动作仍走 resource.py；这里暴露体积感知估算、盘点同步、
版本化队列和文件版本领取等无单一资源 ID 的动作。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from app.services import service_for

router = APIRouter(prefix="/api/compaction", tags=["压缩合并规划"])


def _service():
    service = service_for("compaction")
    assert service is not None
    return service


async def _payload(request: Request) -> Any:
    try:
        return await request.json()
    except Exception:
        return {}


def _split_inventory_options(data: Any) -> tuple[Any, dict[str, Any]]:
    if isinstance(data, list):
        return data, {}
    if not isinstance(data, dict):
        return [], {}
    options = dict(data)
    inventory = options.pop("inventory", None)
    if inventory is None:
        inventory = options.pop("files", None)
    if inventory is None:
        inventory = data
    # 嵌套 options 中的参数优先/合并。
    nested = options.pop("options", None)
    if isinstance(nested, dict):
        merged = dict(nested)
        merged.update(options)
        options = merged
    return inventory, options


@router.post("/plan")
@router.post("/estimate")
@router.post("/candidates")
async def plan_candidates(request: Request) -> dict[str, Any]:
    data = await _payload(request)
    inventory, options = _split_inventory_options(data)
    return _service().estimate_candidates(inventory, **options)


@router.post("/inventory")
@router.post("/sync")
@router.post("/sync-inventory")
async def sync_inventory(request: Request) -> dict[str, Any]:
    data = await _payload(request)
    inventory, options = _split_inventory_options(data)
    return _service().sync_inventory(inventory, **options)


@router.post("/enqueue")
async def enqueue_inventory(request: Request) -> dict[str, Any]:
    data = await _payload(request)
    inventory, options = _split_inventory_options(data)
    return _service().enqueue_inventory(inventory, **options)


@router.get("/queue")
async def queue() -> dict[str, Any]:
    return _service().queue_snapshot()


@router.get("/manifest")
async def manifest() -> dict[str, Any]:
    return _service().manifest_snapshot()


@router.post("/claim-file-version")
@router.post("/claim-version")
async def claim_file_version(request: Request) -> dict[str, Any]:
    data = await _payload(request)
    if not isinstance(data, dict):
        data = {}
    service = _service()
    task_id = data.get("task_id") or data.get("taskId") or data.get("owner")
    file_id = data.get("file_id") or data.get("fileId")
    version = data.get("version")
    file_version = data.get("file_version") or data.get("fileVersion")

    if file_id and (version is not None or file_version):
        result = service.claim_file_version_detailed(
            file_id,
            version if version is not None else file_version,
            task_id=task_id,
            owner=data.get("owner"),
        )
    else:
        result = service.claim_file_version_detailed(
            file_version or version,
            task_id=task_id,
            owner=data.get("owner"),
        )
    return result


@router.post("/claim-next")
@router.post("/claim-next-task")
async def claim_next(request: Request) -> dict[str, Any]:
    data = await _payload(request)
    if not isinstance(data, dict):
        data = {}
    return _service().claim_next(
        task_id=data.get("task_id") or data.get("taskId") or data.get("owner"),
        inventory_version=data.get("inventory_version"),
        table=data.get("table"),
        partition=data.get("partition"),
        include_unselected=bool(data.get("include_unselected", False)),
        owner=data.get("owner"),
    )


@router.post("/claim-group")
@router.post("/claim")
async def claim_group(request: Request) -> dict[str, Any]:
    data = await _payload(request)
    if isinstance(data, list):
        return _service().claim_compaction_group(data)
    if not isinstance(data, dict):
        return {"ok": False, "reason": "invalid_payload"}
    target = data.get("queue_id") or data.get("group_id") or data.get("id") or data.get("file_versions")
    return _service().claim_compaction_group(
        target,
        task_id=data.get("task_id") or data.get("taskId"),
        owner=data.get("owner"),
    )
