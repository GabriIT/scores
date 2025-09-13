import pandas as pd
import re

# Canonical columns the app persists
REQUIRED_COLS = [
    "Project Responsible",
    "Project Status",
    "Potential (t / year)",
    "EST_AY",
    "SOP",
    "ID Number",
]

# Header aliases (case/space/underscore-insensitive)
HEADER_ALIASES = {
    # ID
    "e697663b99b2fa26c1258c9d00450011": "ID Number",
    "id number": "ID Number",

    # SOP
    "enddate project": "SOP",
    "sop": "SOP",

    # PFAM description aliases
    "prod. fam. description": "PFAM",
    "prod. fam.": "PFAM",
    "product family": "PFAM",
    "pfam": "PFAM",

    # other required
    "project responsible": "Project Responsible",
    "project status": "Project Status",
    "potential (t / year)": "Potential (t / year)",
    "est_ay": "EST_AY",
}

# Value normalization for PFAM
PFAM_MAP = {
    "grilamid pa12": "PA12",
    "pa12": "PA12",
    "grilamid  tr": "TR",
    "grilamid tr": "TR",
    "tr": "TR",
    "grivory compounded": "GV",
    "gv": "GV",
    "grivory ht": "HT",
    "ht": "HT",
}

def _normalize_col(c: str) -> str:
    c = (c or "").strip()
    c = re.sub(r"\s+", " ", c)
    key = c.lower().strip()
    key = key.replace("_", " ")
    key = re.sub(r"\s+", " ", key)
    return HEADER_ALIASES.get(key, c)

def _coerce_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0).round(0).astype(int)

def _collapse_duplicate_columns(df: pd.DataFrame, colname: str) -> pd.DataFrame:
    """
    If multiple columns share the same name (e.g., multiple 'PFAM' after aliasing),
    collapse them into a single Series by taking the first non-null across duplicates.
    """
    same = [c for c in df.columns if c == colname]
    if not same:
        return df
    if len(same) == 1:
        # Ensure Series (not DataFrame)
        df[colname] = df[same[0]]
        return df

    # Combine left-to-right preferring non-null
    combined = df[same].bfill(axis=1).iloc[:, 0]
    # Drop all duplicates, then insert one canonical column
    df = df.drop(columns=same)
    df[colname] = combined
    return df

def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    # 1) Normalize column names
    rename_map = {c: _normalize_col(c) for c in df.columns}
    df = df.rename(columns=rename_map)

    # 2) If legacy ID column remains, rename -> ID Number
    if "ID Number" not in df.columns:
        for c in df.columns:
            if c.lower() == "e697663b99b2fa26c1258c9d00450011":
                df = df.rename(columns={c: "ID Number"})
                break

    # 3) Collapse duplicates for all key columns (handles PFAM duplicate crash)
    for key in set(list(HEADER_ALIASES.values()) + REQUIRED_COLS + ["PFAM"]):
        df = _collapse_duplicate_columns(df, key)

    # 4) Ensure required columns exist (create empty if missing)
    for rc in REQUIRED_COLS:
        if rc not in df.columns:
            df[rc] = ""

    # 5) Normalize PFAM values (if present) safely as a Series
    if "PFAM" in df.columns:
        s = df["PFAM"]
        if isinstance(s, pd.DataFrame):
            # Should not happen due to collapse step, but double-guard
            s = s.bfill(axis=1).iloc[:, 0]
        df["PFAM"] = (
            s.astype(str)
             .str.strip()
             .str.lower()
             .map(PFAM_MAP)
             .fillna(s.astype(str).str.strip())
        )

    # 6) Coerce numeric fields to integers
    if "Potential (t / year)" in df.columns:
        df["Potential (t / year)"] = _coerce_int(df["Potential (t / year)"])
    if "EST_AY" in df.columns:
        df["EST_AY"] = _coerce_int(df["EST_AY"])

    # 7) Trim text fields
    for col in ["Project Responsible", "Project Status", "SOP", "ID Number"]:
        if col in df.columns:
            s = df[col]
            if isinstance(s, pd.DataFrame):
                s = s.bfill(axis=1).iloc[:, 0]
            df[col] = s.astype(str).str.strip()

    # Return only the required schema for ingestion
    return df[REQUIRED_COLS].copy()
