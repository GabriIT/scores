from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from jinja2 import Environment, FileSystemLoader, select_autoescape
from datetime import datetime
from typing import List, Optional
import pandas as pd
from io import BytesIO
import os, re
from .db import Base, engine, SessionLocal
from .models import ProjectMonthly
from .scoring import compute_scores_range
from .preprocess import preprocess_df

app = FastAPI(title="KAM_Scores API (preprocess-enabled)")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
Base.metadata.create_all(bind=engine)
templates = Environment(loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")),
                        autoescape=select_autoescape(["html","xml"]))

MONTH_RE = re.compile(r"(\d{4}-\d{2})")
def month_from_filename(name: str) -> Optional[str]:
    m = MONTH_RE.search(name or ""); return m.group(1) if m else None

@app.get("/", response_class=HTMLResponse)
def index(): return templates.get_template("index.html").render()

def _as_int(v) -> int:
    """
    Safely coerce any scalar/array-like to a non-negative int.
    Handles None, '', NaN, numpy scalars, and numeric strings.
    """
    import pandas as pd
    try:
        x = pd.to_numeric(v, errors="coerce")
        # If it's a 0-dim numpy value/Series, collapse to Python scalar
        if hasattr(x, "item"):
            try:
                x = x.item()
            except Exception:
                pass
        # pd.isna works for python, numpy, pandas scalars
        if pd.isna(x):
            return 0
        return int(round(float(x)))
    except Exception:
        # Last-resort parsing
        try:
            return int(round(float(v)))
        except Exception:
            return 0

def _ingest_df_for_month(df: pd.DataFrame, month: str):
    id_col = "ID Number" if "ID Number" in df.columns else None
    if not id_col:
        raise HTTPException(status_code=400, detail="Missing 'ID Number' after preprocessing.")

    rows = []
    for _, r in df.iterrows():
        rows.append(ProjectMonthly(
            month=month,
            kam=(r.get("Project Responsible") or "").strip(),
            status=(r.get("Project Status") or "").strip(),
            potential=_as_int(r.get("Potential (t / year)")),
            est_ay=_as_int(r.get("EST_AY")),
            sop=(r.get("SOP") or "").strip(),
            project_id=(r.get("ID Number") or "").strip()
        ))

    with SessionLocal() as db:
        db.query(ProjectMonthly).filter(ProjectMonthly.month == month).delete()
        db.add_all(rows)
        db.commit()
    return len(rows)

@app.post("/upload/pre")
async def upload_preprocess(month: str = Form(...), file: UploadFile = File(...)):
    try: datetime.strptime(month, "%Y-%m")
    except ValueError: raise HTTPException(status_code=400, detail="month must be YYYY-MM")
    content = await file.read()
    try:
        try:
            df = pd.read_csv(BytesIO(content), sep="\t", dtype=str)
            if df.shape[1]==1: raise ValueError("fallback to comma")
        except Exception:
            df = pd.read_csv(BytesIO(content), sep=",", dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")
    df = preprocess_df(df)
    rows = _ingest_df_for_month(df, month)
    return {"ok": True, "month": month, "rows": rows, "preprocessed": True}

@app.post("/upload/pre/bulk")
async def upload_preprocess_bulk(files: List[UploadFile] = File(...), months: Optional[List[str]] = Form(None)):
    if months and len(months)!=len(files):
        raise HTTPException(status_code=400, detail="months[] length must match files[] length")
    results=[]
    for idx, uf in enumerate(files):
        month = months[idx] if months else month_from_filename(uf.filename)
        if not month: raise HTTPException(status_code=400, detail=f"Cannot infer month from filename: {uf.filename} (need YYYY-MM)")
        content = await uf.read()
        try:
            try:
                df = pd.read_csv(BytesIO(content), sep="\t", dtype=str)
                if df.shape[1]==1: raise ValueError("fallback to comma")
            except Exception:
                df = pd.read_csv(BytesIO(content), sep=",", dtype=str)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to parse {uf.filename}: {e}")
        df = preprocess_df(df)
        rows = _ingest_df_for_month(df, month)
        results.append({"file": uf.filename, "month": month, "rows": rows, "preprocessed": True})
    return {"ok": True, "uploaded": results}

@app.get("/scores")
def get_scores(frm: str, to: str):
    try: start=datetime.strptime(frm,"%Y-%m"); end=datetime.strptime(to,"%Y-%m")
    except ValueError: raise HTTPException(status_code=400, detail="Invalid date. Use YYYY-MM.")
    if end<start: raise HTTPException(status_code=400, detail="'to' must be >= 'from'")
    from .db import SessionLocal
    with SessionLocal() as db:
        scores=compute_scores_range(db, frm, to)
    return JSONResponse(scores)
