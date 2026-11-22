"""错误恢复专用接口：按页归并重采回执、剩余范围领取与恢复结算。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services import service_for
from app.services.error_recover import ErrorRecoverService

router = APIRouter(prefix="/api/error_recover", tags=["错误恢复"])


def _service() -> ErrorRecoverService:
    service = service_for("error_recover")
    if not isinstance(service, ErrorRecoverService):
        raise HTTPException(status_code=404, detail="错误恢复服务未注册")
    return service


class ReceiptPayload(BaseModel):
    page: str
    receipt_no: str | None = None
    shift: str | None = None


@router.post("/{entry_id}/receipts")
def submit_receipt(entry_id: int, payload: ReceiptPayload) -> dict[str, Any]:
    """按页归并重采回执：同一页成功回执再次到达只计算一次。"""
    result, message = _service().submit_recover_receipt(
        entry_id, payload.page, receipt_no=payload.receipt_no, shift=payload.shift)
    if result is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, **result}


@router.get("/{entry_id}/remaining")
def claim_remaining(entry_id: int, shift: str | None = Query(default=None)) -> dict[str, Any]:
    """下一班领取还需恢复的范围：只派发剩余页，已采页面不会重新派出。"""
    result, message = _service().claim_remaining(entry_id, shift=shift)
    if result is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, **result}


@router.get("/{entry_id}/settlement")
def recovery_settlement(entry_id: int) -> dict[str, Any]:
    """恢复结算：补采明细与剩余页清单，支撑批次队列与运行历史对账。"""
    result, message = _service().recovery_settlement(entry_id)
    if result is None:
        raise HTTPException(status_code=404, detail=message)
    return {"ok": True, "message": message, **result}
