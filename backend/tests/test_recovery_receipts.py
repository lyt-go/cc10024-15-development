"""错误恢复按页归并重采回执的集成测试。

场景（与任务描述一致）：
- 同一恢复请求3个失败页，两页先成功、一页迟到，下一班此时领取还需恢复的范围；
- 连续送两次相同的成功回执，只计算一次；再补齐迟到页；
- 比较两次领取时的可执行范围和结算结果，确认没有把已采页面重新派发；
- 确认恢复只登记一次新的运行，批次队列按补采明细+剩余页清单结算实际采集情况，
  运行历史能看出每一页由哪次恢复取得。
"""
from __future__ import annotations

import os
import sys

# 确保后端目录在导入路径中
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from app import persistence as persist

# 清理测试数据目录
for sub in ["runs", "snapshots", "reports", "exports"]:
    p = persist.data_path(sub, "")
    if os.path.isdir(p):
        for f in os.listdir(p):
            os.remove(os.path.join(p, f))


def test_recovery_receipt_flow():
    """两页成功一页迟到：重复回执只算一次，两次领取不重复派发，确认恢复只登记一次运行。"""
    from app.store import store
    from app.services.error_recover import ErrorRecoverService
    from app.services.batch_queue import BatchQueueService

    plan = "SCRI-RECOVER-E2E"
    batch_svc = BatchQueueService()
    batch, _ = batch_svc.create_entry({
        "批次编号": "BATC-RECOVER-E2E",
        "关联计划": plan,
        "任务数量": 3,
    })
    assert batch is not None

    svc = ErrorRecoverService()
    entry, missing = svc.create_entry({
        "任务编号": "ERRO-E2E-0001",
        "关联计划": plan,
        "失败步骤": "data_extract",
        "失败页清单": "PAGE-A,PAGE-B,PAGE-C",
    })
    assert entry is not None, f"登记失败: {missing}"
    rec_id = entry["id"]
    # 失败页清单归一化，剩余页=全部失败页
    assert entry["失败页清单"] == ["PAGE-A", "PAGE-B", "PAGE-C"]
    assert entry["剩余页清单"] == ["PAGE-A", "PAGE-B", "PAGE-C"]
    assert entry["补采明细"] == []

    result, _ = svc.run_action(rec_id, "发起重试")
    assert result["status"] == "重试中"

    # 两页先成功，PAGE-C迟到
    r1, _ = svc.submit_recover_receipt(rec_id, "PAGE-A", receipt_no="RCPT-A")
    r2, _ = svc.submit_recover_receipt(rec_id, "PAGE-B", receipt_no="RCPT-B")
    assert r1["duplicate"] is False and r2["duplicate"] is False
    assert [d["页码"] for d in r2["补采明细"]] == ["PAGE-A", "PAGE-B"]
    assert r2["剩余页清单"] == ["PAGE-C"]

    # 下一班第一次领取：只拿到迟到的PAGE-C，成功页不再派出
    claim1, _ = svc.claim_remaining(rec_id, shift="夜班")
    assert [p["页码"] for p in claim1["可执行范围"]] == ["PAGE-C"]
    # 没补到的页仍能按原失败步骤继续
    assert claim1["可执行范围"][0]["失败步骤"] == "data_extract"
    assert set(claim1["已补采页"]) == {"PAGE-A", "PAGE-B"}
    settle_at_claim1, _ = svc.recovery_settlement(rec_id)
    assert settle_at_claim1["已补采数"] == 2 and settle_at_claim1["剩余数"] == 1

    # 连续送两次相同的成功回执：只计算一次
    dup1, msg1 = svc.submit_recover_receipt(rec_id, "PAGE-A", receipt_no="RCPT-A")
    dup2, msg2 = svc.submit_recover_receipt(rec_id, "PAGE-A", receipt_no="RCPT-A")
    assert dup1["duplicate"] is True and dup2["duplicate"] is True
    assert "只计算一次" in msg1 and "只计算一次" in msg2
    assert len(dup2["补采明细"]) == 2, "重复回执不得增加补采明细"
    assert dup2["剩余页清单"] == ["PAGE-C"]
    # 回执流水留下两次重复记录，便于审计，但补采明细仍按页归并
    flow = store.find("error_recover", rec_id)["回执流水"]
    assert sum(1 for f in flow if f["页码"] == "PAGE-A") == 3

    # 补齐迟到页
    r3, _ = svc.submit_recover_receipt(rec_id, "PAGE-C", receipt_no="RCPT-C")
    assert r3["duplicate"] is False
    assert r3["剩余页清单"] == []
    assert r3["已补采数"] == 3

    # 下一班第二次领取：可执行范围为空
    claim2, _ = svc.claim_remaining(rec_id, shift="夜班")
    assert claim2["可执行范围"] == []
    assert claim2["剩余数"] == 0

    # 比较两次领取：已采页面没有被重新派发
    dispatched_1 = {p["页码"] for p in claim1["可执行范围"]}
    dispatched_2 = {p["页码"] for p in claim2["可执行范围"]}
    recovered_before_claim1 = {"PAGE-A", "PAGE-B"}
    assert not (dispatched_1 & recovered_before_claim1), "第一次领取派出了已采页面"
    assert not (dispatched_2 & (recovered_before_claim1 | {"PAGE-C"})), "第二次领取派出了已采页面"

    # 两次领取时的结算结果对比：差额正好是迟到页PAGE-C
    settle_at_claim2, _ = svc.recovery_settlement(rec_id)
    assert settle_at_claim2["已补采数"] == 3 and settle_at_claim2["剩余数"] == 0
    delta = {d["页码"] for d in settle_at_claim2["补采明细"]} - {
        d["页码"] for d in settle_at_claim1["补采明细"]}
    assert delta == {"PAGE-C"}

    # 确认恢复：只登记一次新的运行
    before = len([r for r in persist.read_jsonl("runs", "run_history.jsonl")
                  if r.get("action") == "auto_create_from_recovery"])
    confirmed, _ = svc.run_action(rec_id, "确认恢复")
    assert confirmed["status"] == "已恢复"
    run_no = confirmed["恢复运行编号"]
    assert run_no, "确认恢复后应登记恢复运行编号"
    # 重复确认不再登记新运行
    confirmed2, _ = svc.run_action(rec_id, "确认恢复")
    assert confirmed2["恢复运行编号"] == run_no
    recovery_runs = [r for r in persist.read_jsonl("runs", "run_history.jsonl")
                     if r.get("action") == "auto_create_from_recovery"]
    assert len(recovery_runs) == before + 1, "确认恢复只应登记一次新的运行"
    store_runs = [r for r in store.rows("run_history") if r.get("记录编号") == run_no]
    assert len(store_runs) == 1

    # 运行历史能看出每一页由哪次恢复取得
    run = store_runs[0]
    assert run["status"] == "已完成"
    assert run["采集行数"] == "3" and run["失败行数"] == "0"
    attribution = {d["页码"]: d["恢复运行"] for d in run["补采明细"]}
    assert attribution == {"PAGE-A": run_no, "PAGE-B": run_no, "PAGE-C": run_no}
    # 任务侧补采明细同样标注了恢复运行
    task_after = store.find("error_recover", rec_id)
    assert all(d["恢复运行"] == run_no for d in task_after["补采明细"])

    # 批次队列按补采明细+剩余页清单更新实际采集情况
    batch_after = store.find("batch_queue", batch["id"])
    assert batch_after["完成数"] == "3"
    assert batch_after["失败数"] == "0"
    settle = batch_after["恢复结算"]
    assert settle["恢复任务"] == "ERRO-E2E-0001"
    assert settle["恢复运行"] == run_no
    assert settle["补采明细"] == ["PAGE-A", "PAGE-B", "PAGE-C"]
    assert settle["剩余页清单"] == []
    batch_runs = [r for r in persist.read_jsonl("runs", "batch_queue.jsonl")
                  if r.get("action") == "recovery_settlement"]
    assert len(batch_runs) == 1, "批次结算只应发生一次"


def test_receipt_guards():
    """回执与领取的边界：清单外页码、已放弃任务、无失败页任务。"""
    from app.store import store
    from app.services.error_recover import ErrorRecoverService

    svc = ErrorRecoverService()
    entry, _ = svc.create_entry({
        "任务编号": "ERRO-GUARD-0001",
        "关联计划": "SCRI-GUARD",
        "失败步骤": "page_snapshot",
        "失败页清单": ["PAGE-X"],
    })
    rec_id = entry["id"]

    # 清单外页码拒收
    result, msg = svc.submit_recover_receipt(rec_id, "PAGE-UNKNOWN")
    assert result is None and "失败页清单" in msg

    # 缺页码拒收
    result, msg = svc.submit_recover_receipt(rec_id, "  ")
    assert result is None and "页码" in msg

    # 不存在的任务
    result, msg = svc.submit_recover_receipt(99999, "PAGE-X")
    assert result is None and "不存在" in msg
    claim, msg = svc.claim_remaining(99999)
    assert claim is None and "不存在" in msg

    # 已放弃任务：拒收回执、拒绝领取
    svc.run_action(rec_id, "标记放弃")
    result, msg = svc.submit_recover_receipt(rec_id, "PAGE-X")
    assert result is None and "已放弃" in msg
    claim, msg = svc.claim_remaining(rec_id)
    assert claim is None and "已放弃" in msg

    # 无失败页的任务：领取范围为空
    entry2, _ = svc.create_entry({
        "任务编号": "ERRO-GUARD-0002",
        "关联计划": "SCRI-GUARD",
        "失败步骤": "data_extract",
    })
    claim2, msg2 = svc.claim_remaining(entry2["id"])
    assert claim2 is not None and claim2["可执行范围"] == []

    # 已恢复任务：新页拒收，重复页仍只算一次
    entry3, _ = svc.create_entry({
        "任务编号": "ERRO-GUARD-0003",
        "关联计划": "SCRI-GUARD",
        "失败步骤": "data_extract",
        "失败页清单": ["PAGE-Y", "PAGE-Z"],
    })
    rec3 = entry3["id"]
    svc.submit_recover_receipt(rec3, "PAGE-Y")
    svc.run_action(rec3, "确认恢复")
    result, msg = svc.submit_recover_receipt(rec3, "PAGE-Z")
    assert result is None and "已恢复" in msg
    dup, _ = svc.submit_recover_receipt(rec3, "PAGE-Y")
    assert dup is not None and dup["duplicate"] is True
    assert dup["已补采数"] == 1


def test_recovery_snapshot_progress():
    """domain_snapshot 输出按页归并的补采进度。"""
    from app.services.error_recover import ErrorRecoverService

    svc = ErrorRecoverService()
    snap = svc.domain_snapshot()
    assert "recovered_pages" in snap and "remaining_pages" in snap
    # 种子数据 ERRO-0002 已补1页剩1页，ERRO-0003 已补1页，ERRO-GUARD-0003 补1页剩1页
    assert snap["recovered_pages"] >= 3
    assert snap["remaining_pages"] >= 1


if __name__ == "__main__":
    test_recovery_receipt_flow()
    print("PASS: test_recovery_receipt_flow")
    test_receipt_guards()
    print("PASS: test_receipt_guards")
    test_recovery_snapshot_progress()
    print("PASS: test_recovery_snapshot_progress")
    print("\nALL 3 RECOVERY RECEIPT TESTS PASSED")
