"""换表流程专项测试：三阶段换表、核对期间并发补报/撤销、409 与回滚。

场景设计（初始图例 LEGEND_V1）：
- V2A：删掉 01->0103、02->0202，快照下各图幅仍可行；核对期间补报一个
  只有旧图例能容纳的复核数 -> 发布前重判出新冲突 -> 409，原图例继续供数；
- V2B：删掉整个 03 旧码，快照下含 03 的图幅全部冲突；核对期间撤掉 03
  计数（含整幅撤空）-> 发布前重判通过 -> 换表生效。
"""
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

LEGEND_V1 = {
    "01": ["0101", "0103"],
    "02": ["0201", "0202"],
    "03": ["0301", "0302"],
}
# 快照下可行；0103/0202 被删后，核对期间补报 0103 会变成冲突
LEGEND_V2A = {
    "01": ["0101"],
    "02": ["0201", "0202"],
    "03": ["0301", "0302"],
}
# 删掉整个 03：快照下所有含 03 计数的图幅都冲突
LEGEND_V2B = {
    "01": ["0101"],
    "02": ["0201", "0202"],
}


@pytest.fixture()
def client(tmp_path):
    app = create_app(str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def seed(client, extra=()):
    assert client.put("/legend", json={"mapping": LEGEND_V1}).status_code == 200
    assert client.put("/sheets/S1/old-counts",
                      json={"counts": {"01": 1000, "02": 500, "03": 800}}
                      ).status_code == 200
    assert client.put("/sheets/S2/old-counts",
                      json={"counts": {"01": 400, "03": 600}}).status_code == 200
    for sid, counts in extra:
        assert client.put(f"/sheets/{sid}/old-counts",
                          json={"counts": counts}).status_code == 200


def _gated_precheck(service, monkeypatch):
    """把全库核对阶段变成可阻塞的闸门，返回 (entered, release)。"""
    entered = threading.Event()
    release = threading.Event()
    original = service._precheck_legend

    def slow_precheck(mapping, snapshot):
        entered.set()
        assert release.wait(10), "核对闸门超时未释放"
        return original(mapping, snapshot)

    monkeypatch.setattr(service, "_precheck_legend", slow_precheck)
    return entered, release


# ---------- 情形一：核对期间补报新增冲突，换表退回且补报仍入库 ----------

def test_supplement_conflict_during_check_detected_at_publish(client):
    """分阶段确定性验证：快照核对通过，期间补报制造冲突，发布被拒。"""
    seed(client)
    svc = client.app.state.service
    v2a = {k: set(v) for k, v in LEGEND_V2A.items()}

    snapshot = svc._snapshot_for_swap()
    assert svc._precheck_legend(v2a, snapshot) == {}  # 快照口径下无冲突

    # 核对期间外业补报：旧图例可行（0103 可由 01 划入），新图例下 0103 无来源
    r = client.put("/sheets/S2/review-totals", json={"totals": {"0103": 300}})
    assert r.status_code == 200

    conflicts = svc._publish_legend(v2a, snapshot, {})
    assert [c["sheet_id"] for c in conflicts] == ["S2"]
    assert any("0103" in p for p in conflicts[0]["problems"])


def test_supplement_conflict_swap_rejected_via_http(client, monkeypatch):
    """端到端：核对期间补报不被阻塞，换表 409，原图例/版号继续供数。"""
    seed(client)
    svc = client.app.state.service
    entered, release = _gated_precheck(svc, monkeypatch)
    outcome = {}

    def do_swap():
        r = client.put("/legend", json={"mapping": LEGEND_V2A})
        outcome["status"] = r.status_code
        outcome["body"] = r.json()

    t = threading.Thread(target=do_swap)
    t.start()
    assert entered.wait(5)  # 已进入全库核对
    assert t.is_alive()     # 换表仍卡在核对阶段

    # 核对期间补报/更正/撤销照常按生效图例提交，不被换表阻塞
    r = client.put("/sheets/S2/review-totals", json={"totals": {"0103": 300}})
    assert r.status_code == 200
    assert t.is_alive()  # 补报返回时换表核对仍未结束

    release.set()
    t.join(5)
    assert not t.is_alive()
    assert outcome["status"] == 409
    affected = {s["sheet_id"] for s in
                outcome["body"]["detail"]["affected_sheets"]}
    assert affected == {"S2"}  # 只列真正冲突的图幅，S1 沿用核对通过结果

    legend = client.get("/legend").json()
    assert legend["version"] == 1  # 版号未变
    assert legend["mapping"] == {k: sorted(v) for k, v in LEGEND_V1.items()}
    # 已入库的补报仍然有效，服务继续按原图例供数
    assert client.get("/sheets/S2").json()["review_totals"] == {"0103": 300}
    ov = client.get("/overlap", params={"sheet_id": "S2", "old_code": "01",
                                        "new_code": "0103"}).json()
    assert ov["legend_version"] == 1 and ov["in_legend"] is True


def test_correction_and_withdrawal_during_check_are_accepted(client, monkeypatch):
    """核对期间更正与撤空复核数同样不被阻塞，且按旧图例校验。"""
    seed(client)
    svc = client.app.state.service
    entered, release = _gated_precheck(svc, monkeypatch)
    t = threading.Thread(target=lambda: client.put(
        "/legend", json={"mapping": LEGEND_V2A}))
    t.start()
    assert entered.wait(5)
    try:
        assert client.put("/sheets/S1/review-totals",
                          json={"totals": {"0101": 600}}).status_code == 200
        # 撤销（空表替换）在核对期间也可提交
        assert client.put("/sheets/S1/review-totals",
                          json={"totals": {}}).status_code == 200
        assert client.get("/sheets/S1").json()["review_totals"] == {}
    finally:
        release.set()
        t.join(5)


# ---------- 情形二：核对期间撤销消除冲突，换表成功 ----------

def test_withdrawal_clears_conflict_and_swap_publishes(client):
    """分阶段：快照下 S1/S2/S3 全冲突；撤掉 03（S3 整幅撤空）后发布成功。"""
    seed(client, extra=[("S3", {"03": 100})])
    svc = client.app.state.service
    v2b = {k: set(v) for k, v in LEGEND_V2B.items()}

    snapshot = svc._snapshot_for_swap()
    precheck = svc._precheck_legend(v2b, snapshot)
    assert set(precheck) == {"S1", "S2", "S3"}  # 全库核对列出全部冲突图幅

    # 核对期间撤销 03 计数；S3 整幅撤空
    assert client.put("/sheets/S1/old-counts",
                      json={"counts": {"01": 1000, "02": 500}}).status_code == 200
    assert client.put("/sheets/S2/old-counts",
                      json={"counts": {"01": 400}}).status_code == 200
    assert client.put("/sheets/S3/old-counts",
                      json={"counts": {}}).status_code == 200

    assert svc._publish_legend(v2b, snapshot, precheck) == []
    legend = client.get("/legend").json()
    assert legend["version"] == 2 and "03" not in legend["mapping"]
    assert client.get("/sheets/S3").status_code == 404  # 撤空图幅不再存在
    ov = client.get("/overlap", params={"sheet_id": "S1", "old_code": "01",
                                        "new_code": "0101"}).json()
    assert ov["legend_version"] == 2


def test_withdrawal_clears_conflict_via_http(client, monkeypatch):
    """端到端：核对期间撤销冲突计数，换表发布成功并递增版号。"""
    seed(client, extra=[("S3", {"03": 100})])
    svc = client.app.state.service
    entered, release = _gated_precheck(svc, monkeypatch)
    outcome = {}

    def do_swap():
        r = client.put("/legend", json={"mapping": LEGEND_V2B})
        outcome["status"] = r.status_code
        outcome["body"] = r.json()

    t = threading.Thread(target=do_swap)
    t.start()
    assert entered.wait(5)
    client.put("/sheets/S1/old-counts",
               json={"counts": {"01": 1000, "02": 500}})
    client.put("/sheets/S2/old-counts", json={"counts": {"01": 400}})
    client.put("/sheets/S3/old-counts", json={"counts": {}})
    release.set()
    t.join(5)

    assert outcome["status"] == 200
    assert outcome["body"]["version"] == 2
    assert client.get("/legend").json()["version"] == 2


def test_unchanged_sheet_keeps_precheck_result(client):
    """未在核对期间变更的图幅沿用全库核对结果，仍冲突则整体退回。"""
    seed(client, extra=[("S4", {"03": 50})])
    svc = client.app.state.service
    v2b = {k: set(v) for k, v in LEGEND_V2B.items()}

    snapshot = svc._snapshot_for_swap()
    precheck = svc._precheck_legend(v2b, snapshot)
    assert set(precheck) == {"S1", "S2", "S4"}

    # 只修 S1、S2，S4 不动
    client.put("/sheets/S1/old-counts",
               json={"counts": {"01": 1000, "02": 500}})
    client.put("/sheets/S2/old-counts", json={"counts": {"01": 400}})

    conflicts = svc._publish_legend(v2b, snapshot, precheck)
    assert [c["sheet_id"] for c in conflicts] == ["S4"]  # 沿用核对结果
    assert client.get("/legend").json()["version"] == 1  # 换表退回，版号不变
    # 已入库的图幅更新仍然有效
    assert client.get("/sheets/S2").json()["old_counts"] == {"01": 400}


def test_new_sheet_added_during_check_is_judged_at_publish(client):
    """核对期间新增的图幅（快照中没有）必须在发布前纳入判定。"""
    seed(client)
    svc = client.app.state.service
    v2b = {k: set(v) for k, v in LEGEND_V2B.items()}
    snapshot = svc._snapshot_for_swap()
    precheck = svc._precheck_legend(v2b, snapshot)
    assert set(precheck) == {"S1", "S2"}

    # 新增图幅只登记新图例无法容纳的 03 计数
    client.put("/sheets/S5/old-counts", json={"counts": {"03": 10}})
    conflicts = svc._publish_legend(v2b, snapshot, precheck)
    assert {c["sheet_id"] for c in conflicts} == {"S1", "S2", "S5"}


# ---------- 写库失败：原图例/版号继续供数，计数更新保留 ----------

def test_publish_write_failure_rolls_back_legend(client, monkeypatch):
    seed(client)
    svc = client.app.state.service
    v2a = {k: set(v) for k, v in LEGEND_V2A.items()}

    class FailingConn:
        """连接代理：仅让换表 INSERT 批量写失败，其余调用透传。"""

        def __init__(self, real):
            self._real = real

        def executemany(self, *args, **kwargs):
            raise sqlite3.OperationalError("simulated disk failure")

        def __getattr__(self, name):
            return getattr(self._real, name)

    monkeypatch.setattr(svc, "conn", FailingConn(svc.conn))
    with pytest.raises(sqlite3.OperationalError):
        svc.replace_legend(v2a)
    monkeypatch.undo()  # 恢复真实连接，验证服务仍可正常供数与写入

    # 原图例与版号不变，服务仍可正常供数与写入
    legend = client.get("/legend").json()
    assert legend["version"] == 1
    assert legend["mapping"] == {k: sorted(v) for k, v in LEGEND_V1.items()}
    assert client.put("/sheets/S2/review-totals",
                      json={"totals": {"0301": 100}}).status_code == 200
    assert client.get("/sheets/S2").json()["review_totals"] == {"0301": 100}


def test_overlap_reads_one_committed_snapshot(client):
    """/overlap 的图例、版号、计数取自同一已提交状态。"""
    seed(client)
    client.put("/sheets/S1/review-totals", json={"totals": {"0101": 600}})
    r = client.put("/legend", json={"mapping": {
        "01": ["0101", "0102", "0103"],
        "02": ["0201", "0202"],
        "03": ["0301", "0302"],
    }})
    assert r.status_code == 200
    ov = client.get("/overlap", params={"sheet_id": "S1", "old_code": "01",
                                        "new_code": "0102"}).json()
    assert ov["legend_version"] == 2 and ov["in_legend"] is True
    assert (ov["min"], ov["max"]) == (0, 400)  # 0101=600 锁定，同快照计数
