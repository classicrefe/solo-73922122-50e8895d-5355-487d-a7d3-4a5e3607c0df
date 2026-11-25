"""国土调查地类拆分核算 API。

功能：
- 登记/替换各图幅旧地类像元数（PUT /sheets/{id}/old-counts）；
- 登记/更正各图幅部分新地类的复核整幅总数（PUT /sheets/{id}/review-totals）；
- 整表更换新旧地类对照表（PUT /legend，一个旧码可对应多个新码）；
- 按图幅 + 一对新旧地类查询重合像元数的最小/最大值（GET /overlap），
  两值相同即为确数。

一致性约定：
- 普通更新先写入事务、再按当前生效图例校验受影响图幅是否仍存在合法拆分；
  只要有图幅无解，整次更新回滚，并在 409 响应中列出受影响图幅；
- 换表分三个阶段，全程不阻塞外业补报/更正/撤销（这些写事务只取写锁，
  不取换表锁，按当时生效图例照常提交）：
  1. 快照：取提交时刻已入库的各图幅计数与当前图例版号；
  2. 全库核对：用新图例在内存中逐图幅核对，不持有任何数据库锁/事务；
  3. 发布：短暂取写锁，重新读取计数，只对核对期间新增、改动或撤空的
     图幅按最新计数重新判定，其余图幅沿用第 2 步结果；仍有冲突则返回
     409 并列出全部冲突图幅，原图例与版号继续供数；
- 换表核对期间已入库的计数更新独立提交，即使换表最终退回也仍然有效；
- 查询只读取已提交状态（全局锁 + 单连接，读写互斥，单次查询看到同一快照）。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field, NonNegativeInt

from .solver import InfeasibleStateError, SheetProblem, check_feasible, overlap_bounds

SCHEMA = """
CREATE TABLE IF NOT EXISTS legend (
    old_code TEXT NOT NULL,
    new_code TEXT NOT NULL,
    PRIMARY KEY (old_code, new_code)
);
CREATE TABLE IF NOT EXISTS old_counts (
    sheet_id TEXT NOT NULL,
    old_code TEXT NOT NULL,
    count INTEGER NOT NULL CHECK (count >= 0),
    PRIMARY KEY (sheet_id, old_code)
);
CREATE TABLE IF NOT EXISTS review_totals (
    sheet_id TEXT NOT NULL,
    new_code TEXT NOT NULL,
    total INTEGER NOT NULL CHECK (total >= 0),
    PRIMARY KEY (sheet_id, new_code)
);
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class LegendBody(BaseModel):
    mapping: dict[str, list[str]] = Field(
        ..., description="旧地类 -> 可对应的新地类列表（一个旧码可对应多个新码），整表替换")


class OldCountsBody(BaseModel):
    counts: dict[str, NonNegativeInt] = Field(
        ..., description="旧地类 -> 像元数，整幅替换；0 等价于不登记")


class ReviewTotalsBody(BaseModel):
    totals: dict[str, NonNegativeInt] = Field(
        ..., description="新地类 -> 复核整幅总数，整幅替换（可更正）；不需要的键省略即可")


def _normalize_codes(pairs: dict[str, list[str]]) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    for old, news in pairs.items():
        old = old.strip()
        if not old:
            raise HTTPException(422, "地类代码不能为空字符串")
        targets = {n.strip() for n in news if n.strip()}
        if targets:
            mapping.setdefault(old, set()).update(targets)
    return mapping


def _normalize_counts(pairs: dict[str, int], label: str) -> dict[str, int]:
    out: dict[str, int] = {}
    for code, value in pairs.items():
        code = code.strip()
        if not code:
            raise HTTPException(422, f"{label}的地类代码不能为空字符串")
        if value > 0:
            out[code] = out.get(code, 0) + value
    return out


class LandService:
    """持有 SQLite 连接与写锁，所有读写都序列化，查询只见已提交状态。"""

    def __init__(self, db_path: str):
        if db_path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False,
                                    isolation_level=None)  # 手动控制事务
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.execute(
            "INSERT OR IGNORE INTO meta (key, value) VALUES ('legend_version', '0')")
        self.conn.commit()
        # 普通计数写事务只取 lock；换表用 swap_lock 串行化，
        # 且全库核对阶段两个锁都不取，因此核对不阻塞任何补报/更正/撤销。
        self.lock = threading.RLock()
        self.swap_lock = threading.RLock()

    # ---- 读取辅助（调用方需已持有 lock） ----
    def _mapping(self) -> dict[str, set[str]]:
        rows = self.conn.execute("SELECT old_code, new_code FROM legend").fetchall()
        mapping: dict[str, set[str]] = {}
        for r in rows:
            mapping.setdefault(r["old_code"], set()).add(r["new_code"])
        return mapping

    def _legend_version(self) -> int:
        row = self.conn.execute(
            "SELECT value FROM meta WHERE key = 'legend_version'").fetchone()
        return int(row["value"])

    def _sheet_problem(self, sheet_id: str,
                       mapping: dict[str, set[str]] | None = None) -> SheetProblem:
        if mapping is None:
            mapping = self._mapping()
        old_counts = {
            r["old_code"]: r["count"]
            for r in self.conn.execute(
                "SELECT old_code, count FROM old_counts WHERE sheet_id = ?",
                (sheet_id,))
        }
        review = {
            r["new_code"]: r["total"]
            for r in self.conn.execute(
                "SELECT new_code, total FROM review_totals WHERE sheet_id = ?",
                (sheet_id,))
        }
        return SheetProblem(old_counts=old_counts, review_totals=review,
                            mapping=mapping)

    def _sheet_ids_with_data(self) -> list[str]:
        rows = self.conn.execute(
            "SELECT sheet_id FROM old_counts UNION SELECT sheet_id FROM review_totals"
        ).fetchall()
        return sorted(r["sheet_id"] for r in rows)

    def _snapshot_counts(self) -> dict[str, tuple[dict[str, int], dict[str, int]]]:
        """读取全部图幅已提交计数的内存快照（调用方需已持有 lock）。"""
        snap: dict[str, list[dict[str, int]]] = {}
        for r in self.conn.execute("SELECT sheet_id, old_code, count FROM old_counts"):
            snap.setdefault(r["sheet_id"], [{}, {}])[0][r["old_code"]] = r["count"]
        for r in self.conn.execute(
                "SELECT sheet_id, new_code, total FROM review_totals"):
            snap.setdefault(r["sheet_id"], [{}, {}])[1][r["new_code"]] = r["total"]
        return {sid: (oc, rv) for sid, (oc, rv) in snap.items()}

    # ---- 普通写操作：事务内先写入再按当前图例校验，失败整体回滚 ----
    def put_old_counts(self, sheet_id: str, counts: dict[str, int]) -> list[dict]:
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute("DELETE FROM old_counts WHERE sheet_id = ?",
                                  (sheet_id,))
                self.conn.executemany(
                    "INSERT INTO old_counts (sheet_id, old_code, count) VALUES (?, ?, ?)",
                    [(sheet_id, o, c) for o, c in sorted(counts.items())])
                affected = self._validate_sheets([sheet_id])
                if affected:
                    self.conn.rollback()
                    return affected
                self.conn.commit()
                return []
            except Exception:
                self.conn.rollback()
                raise

    def put_review_totals(self, sheet_id: str, totals: dict[str, int]) -> list[dict]:
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute("DELETE FROM review_totals WHERE sheet_id = ?",
                                  (sheet_id,))
                self.conn.executemany(
                    "INSERT INTO review_totals (sheet_id, new_code, total) "
                    "VALUES (?, ?, ?)",
                    [(sheet_id, n, v) for n, v in sorted(totals.items())])
                affected = self._validate_sheets([sheet_id])
                if affected:
                    self.conn.rollback()
                    return affected
                self.conn.commit()
                return []
            except Exception:
                self.conn.rollback()
                raise

    def _validate_sheets(self, sheet_ids: list[str]) -> list[dict]:
        """在事务内校验图幅可行性，返回 [{sheet_id, problems}]（空 = 全部可行）。"""
        mapping = self._mapping()
        affected = []
        for sid in sheet_ids:
            problems = check_feasible(self._sheet_problem(sid, mapping))
            if problems:
                affected.append({"sheet_id": sid, "problems": problems})
        return affected

    # ---- 换表：三阶段，核对期间不阻塞计数补报/更正/撤销 ----
    def _snapshot_for_swap(self) -> dict:
        """阶段 1：对已提交状态取快照（提交那一刻已入库的计数为准）。"""
        with self.lock:
            return {
                "version": self._legend_version(),
                "mapping": self._mapping(),
                "counts": self._snapshot_counts(),
            }

    def _precheck_legend(
            self, new_mapping: dict[str, set[str]],
            snapshot: dict) -> dict[str, list[str]]:
        """阶段 2：全库核对。纯内存计算，不取任何数据库锁，
        期间计数写事务可照常提交。返回 {冲突图幅: 问题列表}。"""
        conflicts: dict[str, list[str]] = {}
        for sid, (old_counts, review) in snapshot["counts"].items():
            problems = check_feasible(SheetProblem(
                old_counts=old_counts, review_totals=review, mapping=new_mapping))
            if problems:
                conflicts[sid] = problems
        return conflicts

    def _publish_legend(
            self, new_mapping: dict[str, set[str]], snapshot: dict,
            precheck_conflicts: dict[str, list[str]]) -> list[dict]:
        """阶段 3：取写锁发布。只重新判定核对期间新增/改动/撤空的图幅，
        其余沿用阶段 2 结果。返回冲突列表（空 = 发布成功）。"""
        with self.lock:
            current = self._snapshot_counts()
            changed = {sid for sid in (set(current) | set(snapshot["counts"]))
                       if current.get(sid, ({}, {})) !=
                       snapshot["counts"].get(sid, ({}, {}))}
            conflicts: list[dict] = []
            # 未变更图幅直接沿用全库核对结果（含当时判定的冲突）
            conflicts.extend({"sheet_id": sid, "problems": problems}
                             for sid, problems in sorted(precheck_conflicts.items())
                             if sid not in changed)
            # 核对期间新增、改动或撤空的图幅，按最新计数重新判定
            for sid in sorted(changed):
                problems = check_feasible(
                    self._sheet_problem(sid, new_mapping))
                if problems:
                    conflicts.append({"sheet_id": sid, "problems": problems})
            if conflicts:
                return conflicts
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute("DELETE FROM legend")
                self.conn.executemany(
                    "INSERT INTO legend (old_code, new_code) VALUES (?, ?)",
                    [(o, n) for o, ns in new_mapping.items()
                     for n in sorted(ns)])
                self.conn.execute(
                    "UPDATE meta SET value = CAST(value AS INTEGER) + 1 "
                    "WHERE key = 'legend_version'")
                self.conn.execute(
                    "INSERT OR REPLACE INTO meta (key, value) VALUES "
                    "('legend_updated_at', ?)",
                    (datetime.now(timezone.utc).isoformat(),))
                self.conn.commit()
                return []
            except Exception:
                self.conn.rollback()
                raise

    def replace_legend(self, mapping: dict[str, set[str]]) -> list[dict]:
        """整表换对照表，返回冲突图幅列表（空 = 成功）。

        全库核对在数据库锁之外进行，期间补报/更正/撤销按当前生效图例
        照常提交；发布前只重判核对期间发生变化的图幅。换表被拒或写库
        失败时原图例与版号不变，已入库的图幅更新不受影响。
        """
        with self.swap_lock:  # 多个换表请求串行，普通计数写不经过该锁
            snapshot = self._snapshot_for_swap()
            precheck_conflicts = self._precheck_legend(mapping, snapshot)
            return self._publish_legend(mapping, snapshot, precheck_conflicts)


def create_app(db_path: str | None = None) -> FastAPI:
    service = LandService(
        db_path or os.environ.get("LAND_DB_PATH", "data/land.db"))
    app = FastAPI(
        title="国土调查地类拆分核算 API",
        version="1.1.0",
        description="登记图幅旧地类像元数、复核新地类总数与新旧地类对照表，"
                    "查询任一对新旧地类重合像元数的可能范围（最小/最大，相同即确数）。"
                    "换表全库核对期间计数补报/更正/撤销按当前生效图例照常提交，"
                    "发布前仅重判核对期间发生变化的图幅。",
    )
    app.state.service = service

    def conflict(affected: list[dict], swap: bool = False) -> HTTPException:
        message = ("新对照表会使部分图幅不存在合法的非负整数拆分，"
                   "换表已退回，原图例与版号继续供数，已入库的计数更新仍然有效"
                   if swap else
                   "该更新会使部分图幅不存在合法的非负整数拆分，"
                   "整次更新已回滚，未写入任何修改")
        return HTTPException(status_code=409, detail={
            "message": message,
            "affected_sheets": affected,
        })

    @app.get("/", include_in_schema=False)
    def root():
        return RedirectResponse("/docs")

    @app.get("/legend", summary="查看当前对照表")
    def get_legend():
        with service.lock:
            return {
                "version": service._legend_version(),
                "mapping": {o: sorted(ns) for o, ns in sorted(service._mapping().items())},
            }

    @app.put("/legend",
             summary="整表更换新旧地类对照表（核对期间不阻塞补报，冲突返回 409）")
    def put_legend(body: LegendBody):
        mapping = _normalize_codes(body.mapping)
        affected = service.replace_legend(mapping)
        if affected:
            raise conflict(affected, swap=True)
        with service.lock:
            version = service._legend_version()
        return {"message": "对照表已整表更换", "version": version,
                "mapping": {o: sorted(ns) for o, ns in sorted(mapping.items())}}

    @app.put("/sheets/{sheet_id}/old-counts", summary="登记/替换图幅旧地类像元数")
    def put_old_counts(sheet_id: str, body: OldCountsBody):
        counts = _normalize_counts(body.counts, "旧地类计数")
        affected = service.put_old_counts(sheet_id, counts)
        if affected:
            raise conflict(affected)
        return {"message": "旧地类像元数已登记", "sheet_id": sheet_id,
                "old_counts": counts}

    @app.put("/sheets/{sheet_id}/review-totals", summary="补报/更正图幅新地类复核总数")
    def put_review_totals(sheet_id: str, body: ReviewTotalsBody):
        totals = _normalize_counts(body.totals, "复核总数")
        affected = service.put_review_totals(sheet_id, totals)
        if affected:
            raise conflict(affected)
        return {"message": "复核总数已登记", "sheet_id": sheet_id,
                "review_totals": totals}

    @app.get("/sheets", summary="列出已有数据的图幅")
    def list_sheets():
        with service.lock:
            sheets: dict[str, dict] = {}
            for r in service.conn.execute(
                    "SELECT sheet_id, SUM(count) AS c FROM old_counts GROUP BY sheet_id"):
                sheets[r["sheet_id"]] = {"old_pixels": r["c"], "review_pixels": 0}
            for r in service.conn.execute(
                    "SELECT sheet_id, SUM(total) AS t FROM review_totals GROUP BY sheet_id"):
                sheets.setdefault(r["sheet_id"], {"old_pixels": 0, "review_pixels": 0})
                sheets[r["sheet_id"]]["review_pixels"] = r["t"]
            return {"sheets": [{"sheet_id": sid, **sheets[sid]}
                               for sid in sorted(sheets)]}

    @app.get("/sheets/{sheet_id}", summary="查看图幅已登记的计数")
    def get_sheet(sheet_id: str):
        with service.lock:
            prob = service._sheet_problem(sheet_id)
            if not prob.old_counts and not prob.review_totals:
                raise HTTPException(404, f"图幅 {sheet_id} 没有任何已登记数据")
            return {
                "sheet_id": sheet_id,
                "old_counts": dict(sorted(prob.old_counts.items())),
                "review_totals": dict(sorted(prob.review_totals.items())),
                "total_pixels": prob.total_pixels,
                "reviewed_pixels": prob.reviewed_pixels,
            }

    @app.get("/overlap", summary="查询一对新旧地类重合像元数的最小/最大值")
    def get_overlap(
        sheet_id: str = Query(..., description="图幅号"),
        old_code: str = Query(..., description="旧地类代码"),
        new_code: str = Query(..., description="新地类代码"),
    ):
        with service.lock:
            prob = service._sheet_problem(sheet_id)
            version = service._legend_version()
        if not prob.old_counts and not prob.review_totals:
            raise HTTPException(404, f"图幅 {sheet_id} 没有任何已登记数据")
        base = {"sheet_id": sheet_id, "old_code": old_code, "new_code": new_code,
                "legend_version": version}
        if new_code not in prob.mapping.get(old_code, set()):
            return {**base, "in_legend": False, "min": 0, "max": 0,
                    "exact": True, "value": 0,
                    "note": "该新旧地类对不在对照表中，重合像元数恒为 0"}
        try:
            lo, hi = overlap_bounds(prob, old_code, new_code)
        except InfeasibleStateError as exc:  # 理论上不会发生
            raise HTTPException(500, str(exc))
        return {**base, "in_legend": True, "min": lo, "max": hi,
                "exact": lo == hi, "value": lo if lo == hi else None,
                "note": "确数" if lo == hi else "区间估计，需进一步复核"}

    return app


app = create_app()
