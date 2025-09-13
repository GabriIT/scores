from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from jinja2 import Environment, FileSystemLoader, select_autoescape
from datetime import datetime
import pandas as pd
import os
from io import BytesIO

from .db import Base, engine, SessionLocal
from .models import ProjectMonthly
from .scoring import compute_scores_range

app = FastAPI(title="KAM_Scores API")

# Allow local dev + future subdomains
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create tables
Base.metadata.create_all(bind=engine)

# Templates
templates = Environment(
    loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates")),
    autoescape=select_autoescape(["html", "xml"]),
)

@app.get("/", response_class=HTMLResponse)
def index():
    tpl = templates.get_template("index.html")
    return tpl.render()

@app.post("/upload")
async def upload_dataset(month: str = Form(...), file: UploadFile = File(...)):
    # Validate month
    try:
        datetime.strptime(month, "%Y-%m")
    except ValueError:
        raise HTTPException(status_code=400, detail="month must be YYYY-MM")

    try:
        content = await file.read()
        df = pd.read_csv(BytesIO(content), sep="\t", dtype=str)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse TSV: {e}")

    # Accept either ID Number or the legacy hash column
    id_col = "ID Number" if "ID Number" in df.columns else "E697663B99B2FA26C1258C9D00450011"
    required = ["Project Responsible","Project Status","Potential (t / year)","EST_AY","SOP", id_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing column(s): {', '.join(missing)}")

    rows = []
    for _, r in df.iterrows():
        rows.append(ProjectMonthly(
            month=month,
            kam=(r.get("Project Responsible") or "").strip(),
            status=(r.get("Project Status") or "").strip(),
            potential=int(pd.to_numeric(r.get("Potential (t / year)"), errors="coerce").fillna(0)),
            est_ay=int(pd.to_numeric(r.get("EST_AY"), errors="coerce").fillna(0)),
            sop=(r.get("SOP") or "").strip(),
            project_id=(r.get(id_col) or "").strip()
        ))
    with SessionLocal() as db:
        db.query(ProjectMonthly).filter(ProjectMonthly.month == month).delete()
        db.add_all(rows)
        db.commit()

    return {"ok": True, "month": month, "rows": len(rows)}

@app.get("/scores")
def get_scores(frm: str, to: str):
    try:
        start = datetime.strptime(frm, "%Y-%m")
        end = datetime.strptime(to, "%Y-%m")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date. Use YYYY-MM.")

    if end < start:
        raise HTTPException(status_code=400, detail="'to' must be >= 'from'")

    with SessionLocal() as db:
        scores = compute_scores_range(db, frm, to)
    return JSONResponse(scores)
