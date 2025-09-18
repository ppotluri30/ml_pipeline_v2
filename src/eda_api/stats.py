import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.patches import Rectangle

import dcor
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from scipy.stats import gmean
from statsmodels.graphics.tsaplots import plot_pacf

# ---- light, fast summary for UI ----
def quick_profile(df: pd.DataFrame) -> dict:
    df_num = df.select_dtypes(include=np.number)
    rows = int(df.shape[0]); cols = int(df.shape[1])
    rows_with_any_na = int(df.isna().any(axis=1).sum())
    na_pct_rows = 100.0 * rows_with_any_na / max(1, rows)
    na_pct_cols = {c: float(df[c].isna().mean() * 100.0) for c in df.columns}
    # cheap stats
    desc = df_num.describe().to_dict()
    return {
        "rows": rows,
        "columns": cols,
        "rows_with_na": rows_with_any_na,
        "rows_with_na_pct": na_pct_rows,
        "na_pct_per_column": na_pct_cols,
        "numeric_describe": desc,
    }

# ---- plots (cap features & sample to stay fast) ----
def _cap_numeric(df: pd.DataFrame, max_features=8, sample_rows=50000):
    df_num = df.select_dtypes(include=np.number).copy()
    if sample_rows and len(df_num) > sample_rows:
        df_num = df_num.sample(sample_rows, random_state=42)
    # pick most variable columns to be informative
    if df_num.shape[1] > max_features:
        var = df_num.var(numeric_only=True).sort_values(ascending=False)
        df_num = df_num[var.index[:max_features]]
    return df_num

def corrplot(df: pd.DataFrame):
    df_num = _cap_numeric(df)
    feats = df_num.columns
    n = len(feats)
    if n == 0:
        fig, ax = plt.subplots(figsize=(4,2))
        ax.text(0.5,0.5,"No numeric columns", ha="center", va="center"); ax.axis("off")
        return fig

    df_scaled = pd.DataFrame(StandardScaler().fit_transform(df_num), columns=feats).astype(np.float64)
    fig, axs = plt.subplots(n, n, figsize=(n*1.4, n*1.2), constrained_layout=True)

    for i, r in enumerate(feats):
        for j, c in enumerate(feats):
            ax = axs[i, j]
            if j > i:
                corr_val = dcor.distance_correlation(df_scaled[r].to_numpy(), df_scaled[c].to_numpy())
                color = plt.cm.Greens(corr_val)
                rect = Rectangle((0,0),1,1, transform=ax.transAxes, facecolor=color, edgecolor='white', linewidth=0.2)
                ax.add_patch(rect)
                ax.text(0.5,0.5,f"{corr_val:.2f}", ha="center", va="center", fontsize=10, transform=ax.transAxes)
                ax.axis("off")
            elif i == j:
                sns.kdeplot(data=df_num, x=c, ax=ax, fill=True)
                ax.set_ylabel("")
            else:
                ax.plot(df_num[c], df_num[r], ".", markersize=2, alpha=0.5)
                ax.set_ylabel("")
            if i < n-1: ax.set_xlabel("")
            ax.set_xticks([]); ax.set_yticks([])
            if j == 0: ax.set_ylabel(r, rotation=30, labelpad=10)
    return fig

def pca_plot(df: pd.DataFrame, var_threshold=0.95):
    df_num = _cap_numeric(df, max_features=12)
    if df_num.shape[1] < 2:
        fig, ax = plt.subplots(figsize=(4,2))
        ax.text(0.5,0.5,"Need ≥2 numeric columns", ha="center", va="center"); ax.axis("off")
        return fig
    df_scaled = pd.DataFrame(StandardScaler().fit_transform(df_num), columns=df_num.columns).astype(np.float64)
    pca = PCA().fit(df_scaled)
    cum = np.cumsum(pca.explained_variance_ratio_)
    k = int(np.where(cum >= var_threshold)[0][0]) + 1
    pcs = PCA(n_components=k).fit_transform(df_scaled)
    pc_df = pd.DataFrame(pcs, columns=[f"PC_{i+1}" for i in range(k)])
    pc_scaled = pd.DataFrame(StandardScaler().fit_transform(pc_df), columns=pc_df.columns).astype(np.float64)

    M = pd.DataFrame(index=df_num.columns, columns=pc_df.columns, dtype=float)
    for f in df_num.columns:
        for pc in pc_df.columns:
            M.loc[f, pc] = dcor.distance_correlation(df_scaled[f].to_numpy(), pc_scaled[pc].to_numpy())

    fig, ax = plt.subplots(figsize=(min(12, 2+1.1*len(pc_df.columns)), min(10, 1+0.5*len(M.index))))
    sns.heatmap(M, annot=True, fmt=".2f", cmap="viridis", linewidths=.5, ax=ax)
    ax.set_title(f"Distance correlation (PCA covers ≥{int(var_threshold*100)}% var)")
    plt.tight_layout()
    return fig

def pacf_plot(df: pd.DataFrame, maxlags=15):
    df_num = _cap_numeric(df, max_features=6)
    feats = df_num.columns
    if len(feats) == 0:
        fig, ax = plt.subplots(figsize=(4,2))
        ax.text(0.5,0.5,"No numeric columns", ha="center", va="center"); ax.axis("off")
        return fig
    rows = len(feats)
    fig, axes = plt.subplots(rows, 1, figsize=(12, max(6, rows*2.2)), sharex=True)
    axes = np.atleast_1d(axes)
    for i, col in enumerate(feats):
        plot_pacf(df_num[col], lags=maxlags, ax=axes[i], title=f"{col}")
    fig.supxlabel('Lag'); fig.supylabel('PACF'); plt.tight_layout()
    return fig
