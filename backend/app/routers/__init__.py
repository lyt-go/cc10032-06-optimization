"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import compaction_api, resource

# 专用 /api/compaction/... 必须先于 /api/{module}/{entry_id} 动态 GET 注册。
ROUTERS = [compaction_api, resource]
