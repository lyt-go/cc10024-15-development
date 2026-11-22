"""错误恢复领域服务：采集失败后的重试与恢复，消费batch_queue失败信息，写入run_history。

按页归并重采回执：
- 恢复任务登记失败页清单，回执按页归并，同一页成功回执再次到达只计算一次；
- 产出补采明细与剩余页清单，下一班只能领取剩余页，已采页面不会重新派发；
- 确认恢复时同一任务只登记一次新的运行，批次队列按补采明细+剩余页清单结算
  实际采集情况，运行历史可追溯到每一页由哪次恢复取得。
"""
from __future__ import annotations

import re
from typing import Any

from app.store import store
from app.services.base import BaseModuleService, register_hook, _version_id
from app import persistence as persist

_PAGE_SPLIT = re.compile(r"[,，、;；\s]+")


def _normalize_pages(raw: Any) -> list[str]:
    """把失败页清单归一成有序去重的页码列表。"""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        items = [str(x).strip() for x in raw]
    else:
        items = _PAGE_SPLIT.split(str(raw))
    pages: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item and item not in seen:
            seen.add(item)
            pages.append(item)
    return pages


def _recovered_details(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """补采明细：每一页的补采时间、回执编号与恢复运行归属。"""
    details = entry.get("补采明细")
    if isinstance(details, list):
        return details
    entry["补采明细"] = []
    return entry["补采明细"]


def _remaining_pages(entry: dict[str, Any]) -> list[str]:
    """剩余页清单：失败页中还没有成功回执的页。"""
    remaining = entry.get("剩余页清单")
    if isinstance(remaining, list):
        return remaining
    recovered = {str(d.get("页码")) for d in _recovered_details(entry)}
    entry["剩余页清单"] = [p for p in _normalize_pages(entry.get("失败页清单")) if p not in recovered]
    return entry["剩余页清单"]


def _settle_batch_queue(
    entry: dict[str, Any],
    details: list[dict[str, Any]],
    remaining: list[str],
    run_no: str,
) -> None:
    """状态变更为恢复完成时，批次队列按补采明细与剩余页清单更新实际采集情况。"""
    plan = entry.get("关联计划")
    if not plan:
        return
    now = persist.now_iso()
    for batch in store.rows("batch_queue"):
        if batch.get("关联计划") != plan:
            continue
        batch["完成数"] = str(len(details))
        batch["失败数"] = str(len(remaining))
        batch["恢复结算"] = {
            "恢复任务": entry.get("任务编号"),
            "恢复运行": run_no,
            "补采明细": [d.get("页码") for d in details],
            "剩余页清单": list(remaining),
            "结算时间": now,
        }
        batch["updated_at"] = now
        batch["version"] = _version_id(batch)
        persist.append_jsonl("runs", "batch_queue.jsonl", {
            "module": "batch_queue", "entity": "采集批次", "entry_id": batch.get("id"),
            "status": batch.get("status"), "version": batch.get("version", ""),
            "action": "recovery_settlement", "timestamp": now, "abnormal": False,
            "source_module": "error_recover", "source_id": entry.get("id"),
            "完成数": batch["完成数"], "失败数": batch["失败数"],
        })


def _record_recovery_to_history(entry: dict[str, Any]) -> None:
    """恢复任务确认恢复后写入run_history；同一任务只登记一次新的运行。"""
    if entry.get("恢复运行编号"):
        # 已登记过恢复运行，重复确认不再产生新记录
        persist.append_jsonl("runs", "error_recover.jsonl", {
            "module": "error_recover", "entity": "恢复任务", "entry_id": entry.get("id"),
            "status": entry.get("status"), "version": entry.get("version", ""),
            "action": "重复确认恢复", "timestamp": persist.now_iso(), "abnormal": False,
            "恢复运行编号": entry.get("恢复运行编号"),
        })
        return
    details = [dict(d) for d in _recovered_details(entry)]
    remaining = list(_remaining_pages(entry))
    now = persist.now_iso()
    run_rows = store.rows("run_history")
    new_id = max((int(r.get("id", 0)) for r in run_rows), default=0) + 1
    run_no = f"RUN-RECOVER-{new_id:04d}"
    # 运行历史能看出每一页由哪次恢复取得
    for d in details:
        d["恢复运行"] = run_no
    record = {
        "id": new_id,
        "记录编号": run_no,
        "关联计划": entry.get("关联计划", ""),
        "开始时间": now,
        "结束时间": now,
        "采集行数": str(len(details)),
        "失败行数": str(len(remaining)),
        "执行日志": f"从error_recover任务{entry.get('任务编号')}恢复，补采{len(details)}页，剩余{len(remaining)}页",
        "异常摘要": entry.get("错误类型", ""),
        "补采明细": details,
        "剩余页清单": remaining,
        "status": "已完成" if not remaining else "部分成功",
        "pending": bool(remaining),
        "abnormal": False,
        "version": "",
        "updated_at": now,
    }
    run_rows.append(record)
    # 回写任务侧：每页补采明细标注恢复运行，任务只登记这一次运行
    entry["恢复运行编号"] = run_no
    for d in _recovered_details(entry):
        d["恢复运行"] = run_no
    entry["updated_at"] = now
    entry["version"] = _version_id(entry)
    persist.append_jsonl("runs", "run_history.jsonl", {
        "module": "run_history", "entity": "运行记录", "entry_id": new_id,
        "status": record["status"], "version": "", "action": "auto_create_from_recovery",
        "timestamp": now, "abnormal": False,
        "source_module": "error_recover", "source_id": entry.get("id"),
        "补采明细": details, "剩余页清单": remaining,
    })
    persist.append_jsonl("runs", "error_recover.jsonl", {
        "module": "error_recover", "entity": "恢复任务", "entry_id": entry.get("id"),
        "status": entry.get("status"), "version": entry.get("version", ""),
        "action": "恢复运行登记", "timestamp": now, "abnormal": False,
        "恢复运行编号": run_no, "补采页数": len(details), "剩余页数": len(remaining),
    })
    _settle_batch_queue(entry, details, remaining, run_no)


register_hook("error_recover", "确认恢复", _record_recovery_to_history)


class ErrorRecoverService(BaseModuleService):
    MODULE = "error_recover"
    ENTITY = "恢复任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "关联计划", "失败步骤", "失败页清单", "错误类型", "重试边界", "回滚策略", "已重试次数", "恢复状态"]
    REQUIRED_FIELDS = ["任务编号", "关联计划", "失败步骤"]
    STATUS_ORDER = ["待处理", "重试中", "已恢复", "已放弃"]
    ACTION_RULES = {"发起重试": "重试中", "确认恢复": "已恢复", "标记放弃": "已放弃"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记恢复任务：失败页清单归一化，初始化补采明细与剩余页清单。"""
        values = dict(values)
        pages = _normalize_pages(values.get("失败页清单"))
        values["失败页清单"] = pages
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing
        entry["补采明细"] = []
        entry["剩余页清单"] = list(pages)
        entry["回执流水"] = []
        entry["恢复运行编号"] = ""
        entry["version"] = _version_id(entry)
        return entry, []

    def submit_recover_receipt(
        self,
        entry_id: int,
        page: str,
        *,
        receipt_no: str | None = None,
        shift: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """按页归并重采回执：同一页成功回执只计算一次，产出补采明细与剩余页清单。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        page = str(page or "").strip()
        if not page:
            return None, "重采回执缺少页码"
        task_no = entry.get("任务编号")
        failed_pages = _normalize_pages(entry.get("失败页清单"))
        if page not in failed_pages:
            return None, f"页码「{page}」不在{self.ENTITY}{task_no}的失败页清单内"
        if entry.get("status") == "已放弃":
            return None, f"{self.ENTITY}{task_no}已放弃，拒收页码「{page}」的回执"
        details = _recovered_details(entry)
        duplicate = any(str(d.get("页码")) == page for d in details)
        if not duplicate and entry.get("status") == "已恢复":
            return None, f"{self.ENTITY}{task_no}已恢复结算，页码「{page}」请登记新的恢复任务"
        now = persist.now_iso()
        flow = entry.get("回执流水")
        if not isinstance(flow, list):
            flow = entry["回执流水"] = []
        receipt_no = (receipt_no or "").strip() or f"RCPT-{entry_id:04d}-{len(flow) + 1:03d}"
        flow.append({
            "页码": page, "回执编号": receipt_no, "接收时间": now,
            "duplicate": duplicate, "班次": shift or "",
        })
        if not duplicate:
            details.append({
                "页码": page,
                "失败步骤": entry.get("失败步骤", ""),
                "补采时间": now,
                "回执编号": receipt_no,
                "恢复运行": entry.get("恢复运行编号", ""),
            })
            entry["剩余页清单"] = [p for p in _remaining_pages(entry) if p != page]
        entry["updated_at"] = now
        entry["version"] = _version_id(entry)
        persist.append_jsonl("runs", "error_recover.jsonl", {
            "module": self.MODULE, "entity": self.ENTITY, "entry_id": entry_id,
            "status": entry.get("status"), "version": entry.get("version", ""),
            "action": "重采回执", "timestamp": now, "abnormal": False,
            "页码": page, "回执编号": receipt_no, "duplicate": duplicate,
        })
        remaining = list(_remaining_pages(entry))
        result = {
            "任务编号": task_no,
            "页码": page,
            "回执编号": receipt_no,
            "duplicate": duplicate,
            "补采明细": [dict(d) for d in details],
            "剩余页清单": remaining,
            "已补采数": len(details),
            "剩余数": len(remaining),
        }
        message = f"页码「{page}」回执重复，只计算一次" if duplicate else f"页码「{page}」补采成功"
        return result, message

    def claim_remaining(
        self, entry_id: int, *, shift: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """下一班领取还需恢复的范围：只派发剩余页，已采页面不会重新派出。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        if entry.get("status") == "已放弃":
            return None, f"{self.ENTITY}{entry.get('任务编号')}已放弃，无可领取范围"
        remaining = list(_remaining_pages(entry))
        details = _recovered_details(entry)
        failed_step = entry.get("失败步骤", "")
        now = persist.now_iso()
        # 没补到的页仍能按原失败步骤继续
        claim = {
            "任务编号": entry.get("任务编号"),
            "关联计划": entry.get("关联计划"),
            "失败步骤": failed_step,
            "可执行范围": [{"页码": p, "失败步骤": failed_step} for p in remaining],
            "已补采页": [d.get("页码") for d in details],
            "剩余数": len(remaining),
            "班次": shift or "",
            "领取时间": now,
        }
        persist.append_jsonl("runs", "error_recover.jsonl", {
            "module": self.MODULE, "entity": self.ENTITY, "entry_id": entry_id,
            "status": entry.get("status"), "version": entry.get("version", ""),
            "action": "领取剩余范围", "timestamp": now, "abnormal": False,
            "可执行范围": remaining, "剩余数": len(remaining), "班次": shift or "",
        })
        return claim, f"可领取{len(remaining)}页待恢复"

    def recovery_settlement(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """恢复结算：补采明细与剩余页清单，支撑批次队列与运行历史对账。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        details = _recovered_details(entry)
        remaining = _remaining_pages(entry)
        failed_pages = _normalize_pages(entry.get("失败页清单"))
        result = {
            "任务编号": entry.get("任务编号"),
            "关联计划": entry.get("关联计划"),
            "恢复状态": entry.get("status"),
            "失败页清单": failed_pages,
            "补采明细": [dict(d) for d in details],
            "剩余页清单": list(remaining),
            "失败页总数": len(failed_pages),
            "已补采数": len(details),
            "剩余数": len(remaining),
            "恢复运行编号": entry.get("恢复运行编号", ""),
        }
        return result, "恢复结算明细已生成"

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        tasks = store.rows(self.MODULE)
        # 关联的batch_queue统计数据
        batches = store.rows("batch_queue")
        plan_ids = {r.get("关联计划") for r in tasks}
        base["linked_batches"] = sum(1 for b in batches if b.get("关联计划") in plan_ids)
        # 恢复关联的run_history
        runs = persist.read_jsonl("runs", "run_history.jsonl")
        recovery_runs = [r for r in runs if "recover" in str(r.get("action", ""))]
        base["recovery_runs"] = len(recovery_runs)
        base["retry_total"] = sum(
            int(str(r.get("已重试次数") or "0")) for r in tasks if str(r.get("已重试次数") or "").isdigit())
        # 按页归并的补采进度
        base["recovered_pages"] = sum(len(_recovered_details(r)) for r in tasks)
        base["remaining_pages"] = sum(len(_remaining_pages(r)) for r in tasks)
        return base
