from fastapi import FastAPI, HTTPException, Query
import pandas as pd
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, Response
import io, os, json
from .manifest import load_manifest
from .s3_utils import storage_options
from .stats import quick_profile, corrplot, pca_plot, pacf_plot
import re

EDA_MAX_ROWS = int(os.getenv("EDA_MAX_ROWS", "200000"))
import os
import fsspec
from urllib.parse import urlparse

S3_BUCKET = os.getenv("S3_BUCKET", "mlstore")
DOMAIN    = os.getenv("DOMAIN", "traffic")

def _fs():
    return fsspec.filesystem(
        "s3",
        key=os.getenv("S3_ACCESS_KEY"),
        secret=os.getenv("S3_SECRET_KEY"),
        client_kwargs={"endpoint_url": os.getenv("S3_ENDPOINT"),
                       "region_name": os.getenv("S3_REGION", "us-east-1")},
        config_kwargs={"s3": {"addressing_style": "path"}},
    )

import re

def list_manifests(limit: int = 20):
    """Return newest→oldest unique datasets for DOMAIN, preferring stable paths."""
    fs = _fs()
    prefix = f"s3://{S3_BUCKET}/manifests/{DOMAIN}/"
    try:
        paths = [p if str(p).startswith("s3://") else f"s3://{p}"
                 for p in fs.find(prefix) if str(p).endswith("/manifest.json")]
    except Exception:
        paths = []

    def ds_from_path(p: str) -> str:
        return p.split(f"/manifests/{DOMAIN}/", 1)[-1].split("/manifest.json", 1)[0]

    def base_id(ds: str) -> str:
        m = re.match(r"^(.*?)-(\d{8}-\d{6})$", ds)
        return m.group(1) if m else ds

    # gather candidates per base id
    groups = {}
    for p in paths:
        ds = ds_from_path(p)
        if ds.startswith("__root__-") or ds.startswith("demo-"):
            continue
        try:
            info = fs.info(p)
            ts = info.get("LastModified") or info.get("mtime") or ""
        except Exception:
            ts = ""
        b = base_id(ds)
        groups.setdefault(b, []).append((p, ts, ds))

    chosen = {}
    for b, items in groups.items():
        # prefer stable if present
        stable = f"s3://{S3_BUCKET}/manifests/{DOMAIN}/{b}/manifest.json"
        if any(p == stable for (p, _, _) in items):
            chosen[b] = (stable, max((ts for (p, ts, _) in items if p == stable), default=""))
        else:
            # else pick newest timestamped
            p, ts, _ = max(items, key=lambda t: t[1])
            chosen[b] = (p, ts)

    # newest first
    ordered = sorted(chosen.items(), key=lambda kv: kv[1][1], reverse=True)[:limit]
    return [{"dataset_id": b, "manifest_uri": uri} for b, (uri, _) in ordered]




    def sort_key(p: str):
        try:
            info = fs.info(p)
            # Prefer LastModified (s3fs) then mtime if present
            t = info.get("LastModified") or info.get("mtime")
            if t:
                return t
        except Exception:
            pass
        # Fallback: sort by dataset_id’s trailing -YYYYMMDD-HHMMSS if present
        ds = p.split(f"/manifests/{DOMAIN}/", 1)[-1].split("/manifest.json", 1)[0]
        m = re.search(r"-(\d{8}-\d{6})$", ds)
        return m.group(1) if m else ds

    items.sort(key=sort_key, reverse=True)
    out = []
    for p in items[:limit]:
        ds_id = p.split(f"/manifests/{DOMAIN}/", 1)[-1].split("/manifest.json", 1)[0]
        out.append({"dataset_id": ds_id, "manifest_uri": p})
    return out

def _resolve_manifest(manifest_uri: str | None, ds: str | None):
    if manifest_uri:
        return manifest_uri
    cat = list_manifests(limit=50)
    if not cat:
        raise HTTPException(status_code=404, detail="No manifests found. Run PREP first.")
    if ds in (None, "", "latest"):
        return cat[0]["manifest_uri"]
    # match on dataset_id (exact)
    for row in cat:
        if row["dataset_id"] == ds:
            return row["manifest_uri"]
    raise HTTPException(status_code=404, detail=f"dataset_id '{ds}' not found")

def _fig_png_response(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    data = buf.getvalue()
    buf.close()
    try:
        import matplotlib.pyplot as plt
        plt.close(fig)
    except Exception:
        pass
    return Response(content=data, media_type="image/png")
app = FastAPI(title="EDA API", description="Light EDA on datasets via PREP manifest.")

def _load_df(manifest_uri: str, split: str = "train") -> pd.DataFrame:
    man = load_manifest(manifest_uri)
    path = man["artifacts"]["features_train"] if split == "train" else man["artifacts"]["features_val"]
    so = storage_options(path)
    df = pd.read_parquet(path, storage_options=so)
    if EDA_MAX_ROWS and len(df) > EDA_MAX_ROWS:
        df = df.sample(EDA_MAX_ROWS, random_state=42).reset_index(drop=True)
    return df

@app.get("/", response_class=HTMLResponse)
def index(manifest_uri: str | None = None, ds: str | None = None):
    try:
        # pick manifest (ds=latest by default)
        manifest_uri = _resolve_manifest(manifest_uri, ds)
        df = _load_df(manifest_uri, "train")
        prof = quick_profile(df)
        catalog = list_manifests(limit=20)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load dataset: {e}")

    rows = prof["rows"]; cols = prof["columns"]; na_rows = prof["rows_with_na"]; na_pct = prof["rows_with_na_pct"]
    # options for the dropdown (value = manifest_uri, label = dataset_id)
    options = "\n".join(
        f'<option value="{row["manifest_uri"]}" {"selected" if row["manifest_uri"]==manifest_uri else ""}>{row["dataset_id"]}</option>'
        for row in catalog
    )

    html = f"""
    <html><head><title>EDA</title>
    <style>
      body {{ font-family: system-ui, Arial; padding: 16px; max-width: 1200px; margin:auto; }}
      .kpi {{ display:flex; gap:16px; margin: 12px 0; flex-wrap:wrap }}
      .kpi div {{ border:1px solid #ddd; padding:12px; border-radius:8px; min-width:140px }}
      img {{ border:1px solid #ddd; max-width:100%; }}
      code {{ background:#f5f5f5; padding:2px 4px; border-radius:4px; }}
      .row {{ margin: 16px 0; }}
    </style>
    <script>
      function onPick(e) {{
        const val = e.target.value;
        const u = new URL(window.location.href);
        u.searchParams.set('manifest_uri', val);
        u.searchParams.delete('ds');
        window.location = u.toString();
      }}
    </script>
    </head><body>
      <h1>EDA</h1>

      <div class="row">
        <label for="pick"><b>Select dataset</b></label><br/>
        <select id="pick" onchange="onPick(event)">{options}</select>
      </div>

      <div class="row">
        <b>Dataset manifest:</b> <code>{manifest_uri}</code>
      </div>

      <div class="kpi">
        <div><b>Rows</b><br>{rows}</div>
        <div><b>Columns</b><br>{cols}</div>
        <div><b>Rows with NA</b><br>{na_rows} ({na_pct:.2f}%)</div>
      </div>

      <h2>Plots</h2>
      <h3>Distance Correlation</h3>
      <img src="/plot/corr.png?manifest_uri={manifest_uri}" />
      <h3>PCA Heatmap</h3>
      <img src="/plot/pca.png?manifest_uri={manifest_uri}" />
      <h3>PACF (first 15 lags)</h3>
      <img src="/plot/pacf.png?manifest_uri={manifest_uri}" />
    </body></html>
    """
    return HTMLResponse(content=html)

@app.get("/profile.json")
def profile(manifest_uri: str = Query(...)):
    try:
        df = _load_df(manifest_uri, "train")
        prof = quick_profile(df)
        return JSONResponse(prof)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/plot/corr.png")
def plot_corr(manifest_uri: str = Query(...)):
    try:
        df = _load_df(manifest_uri, "train")
        fig = corrplot(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"corrplot failed: {e}")
    return _fig_png_response(fig)

@app.get("/plot/pca.png")
def plot_pca(manifest_uri: str = Query(...)):
    try:
        df = _load_df(manifest_uri, "train")
        fig = pca_plot(df)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"pca failed: {e}")
    return _fig_png_response(fig)

@app.get("/plot/pacf.png")
def plot_pacf_route(manifest_uri: str = Query(...)):
    try:
        df = _load_df(manifest_uri, "train")
        fig = pacf_plot(df, maxlags=15)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"pacf failed: {e}")
    return _fig_png_response(fig)
@app.get("/manifests.json")
def manifests(limit: int = 20):
    return JSONResponse(list_manifests(limit=limit))
