from sqlalchemy.orm import Session
from sqlalchemy import select
from collections import defaultdict
from datetime import datetime
from dateutil.relativedelta import relativedelta

from .models import ProjectMonthly

# Targets (same for all KAMs)
TARGET_PP = 30  # tons
TARGET_LVP = 20 # tons

def _parse_sop_month(s: str):
    if not s:
        return None
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%m/%Y", "%Y-%m"):
        try:
            dt = datetime.strptime(s, fmt)
            return datetime(dt.year, dt.month, 1)
        except:
            continue
    return None

def month_iter(frm: str, to: str):
    start = datetime.strptime(frm, "%Y-%m")
    end = datetime.strptime(to, "%Y-%m")
    cur = start
    while cur <= end:
        yield cur.strftime("%Y-%m")
        cur += relativedelta(months=1)

def fetch_month(db: Session, ym: str):
    rows = db.execute(select(ProjectMonthly).where(ProjectMonthly.month == ym)).scalars().all()
    by_kam = defaultdict(list)
    for r in rows:
        by_kam[r.kam].append(r)
    return by_kam

def compute_scores_range(db: Session, frm: str, to: str):
    months = list(month_iter(frm, to))
    month_data = {ym: fetch_month(db, ym) for ym in months}
    results = {"months": months, "per_kam": {}}

    for ym in months:
        prev_month = None
        idx = months.index(ym)
        if idx > 0:
            prev_month = months[idx-1]

        by_kam = month_data.get(ym, {})
        prev_by_kam = month_data.get(prev_month, {}) if prev_month else {}

        for kam, rows in by_kam.items():
            if kam not in results["per_kam"]:
                results["per_kam"][kam] = {"monthly": {}, "cumulative": 0}

            current = {r.project_id: r for r in rows}
            previous = prev_by_kam.get(kam, {})
            previous = {r.project_id: r for r in previous} if previous else {}

            lvp = 0
            pp_prev = sum(r.potential for r in previous.values() if r.status == "N")
            pp_curr_raw = sum(r.potential for r in current.values() if r.status == "N")
            pp_added = 0
            new_projects_count = 0
            sop_delay_pen = 0
            vol_decrease_pen = 0

            pids = set(current.keys()) | set(previous.keys())
            for pid in pids:
                cur = current.get(pid)
                prv = previous.get(pid)

                if cur and not prv:
                    if cur.status == "N":
                        pp_added += cur.potential
                    new_projects_count += 1

                if cur and prv:
                    if prv.status == "N" and cur.status == "+":
                        lvp += prv.potential

                    if cur.status == "N" and prv.status == "N" and cur.potential > prv.potential:
                        pp_added += (cur.potential - prv.potential)

                    if cur.est_ay < prv.est_ay:
                        vol_decrease_pen += 2 * (prv.est_ay - cur.est_ay)

                    if (prv.status == "+" or cur.status == "+"):
                        sop_prev = _parse_sop_month(prv.sop) if prv else None
                        sop_cur = _parse_sop_month(cur.sop) if cur else None
                        if sop_prev and sop_cur and sop_cur > sop_prev:
                            months_delay = (sop_cur.year - sop_prev.year) * 12 + (sop_cur.month - sop_prev.month)
                            if months_delay > 0:
                                foc_2026 = prv.est_ay
                                sop_delay_pen += months_delay * foc_2026

            pp_expected_after = pp_prev + pp_added - lvp
            pp_shortfall = max(0, pp_expected_after - pp_curr_raw)
            pp_decrease_pen = 2 * pp_shortfall

            pp_gain = 200 if pp_added >= int(TARGET_PP * 1.3) else (100 if pp_added >= TARGET_PP else 0)
            lvp_gain = 400 if lvp >= int(TARGET_LVP * 1.3) else (200 if lvp >= TARGET_LVP else 0)

            no_new_pen = 0 if new_projects_count >= 1 else 100

            month_score = pp_gain + lvp_gain - sop_delay_pen - vol_decrease_pen - pp_decrease_pen - no_new_pen

            results["per_kam"][kam]["monthly"][ym] = {
                "PP_added": pp_added,
                "PP_prev": pp_prev,
                "PP_curr_raw": pp_curr_raw,
                "LVP": lvp,
                "PP_expected_after": pp_expected_after,
                "PP_shortfall": pp_shortfall,
                "gains": {"PP_gain": pp_gain, "LVP_gain": lvp_gain},
                "penalties": {
                    "SOP_delay": sop_delay_pen,
                    "Volume_decrease": vol_decrease_pen,
                    "PP_decrease": pp_decrease_pen,
                    "No_new_project": no_new_pen
                },
                "month_score": month_score
            }

            results["per_kam"][kam]["cumulative"] += month_score

    return results
