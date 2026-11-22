"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import recovery, resource

# recovery 是 error_recover 的专用路由，放在通用资源路由之前注册。
ROUTERS = [recovery, resource]
