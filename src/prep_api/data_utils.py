# src/prep_api/data_utils.py
# Minimal, production-friendly version of the intern's preprocessing
# Steps: (optional clip) -> KNN impute -> scale (fit/transform) -> optional add constant -> cyclic time features
from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Optional, List, Tuple, Any
from sklearn.impute import KNNImputer
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler, MaxAbsScaler

# -----------------------------
# Helpers used by apply_legacy_pipeline
# -----------------------------
def clip_outliers(df: pd.DataFrame, cols: Optional[List[str]] = None,
                  method: str = "percentile", factor: float = 0.1) -> pd.DataFrame:
    X = df.select_dtypes(include=np.number).copy()
    if cols is None:
        cols = X.columns.tolist()
    for col in cols:
        if method == "iqr":
            q1, q3 = X[col].quantile(0.25), X[col].quantile(0.75)
            iqr = q3 - q1
            lower, upper = q1 - factor * iqr, q3 + factor * iqr
        elif method == "percentile":
            lower, upper = X[col].quantile(factor), X[col].quantile(1 - factor)
        else:
            raise ValueError("method must be 'iqr' or 'percentile'")
        X[col] = X[col].clip(lower=lower, upper=upper)
    return X

def handle_nans(df: pd.DataFrame, threshold: float = 0.33, window: int = 2, no_drop: bool = True) -> pd.DataFrame:
    """
    Drop all-NaN columns; optionally drop rows with too many NaNs; then KNN-impute.
    NOTE: Impute on the full numeric matrix (better than per-column).
    """
    X = df.select_dtypes(include=np.number).copy()

    # drop columns that are entirely NaN
    X = X.dropna(axis=1, how="all")

    # optionally drop rows with too many NaNs
    if not no_drop and X.shape[1] > 0:
        thresh_not_missing = int(np.ceil(X.shape[1] * (1 - threshold)))
        X = X.dropna(axis=0, thresh=thresh_not_missing)

    if X.empty:
        return X

    imputer = KNNImputer(n_neighbors=max(1, int(window)), weights="distance", copy=True)
    arr = imputer.fit_transform(X.values)
    X_imputed = pd.DataFrame(arr, index=X.index, columns=X.columns)
    return X_imputed

def _make_scaler(name_or_obj: Any) -> Any:
    if name_or_obj is None:
        return None
    if isinstance(name_or_obj, str):
        name = name_or_obj.strip()
        if name == "StandardScaler":
            return StandardScaler()
        if name == "MinMaxScaler":
            return MinMaxScaler()
        if name == "RobustScaler":
            return RobustScaler()
        if name == "MaxAbsScaler":
            return MaxAbsScaler()
        raise ValueError("Unsupported scaler string.")
    # already a scaler object
    if isinstance(name_or_obj, (StandardScaler, MinMaxScaler, RobustScaler, MaxAbsScaler)):
        return name_or_obj
    raise TypeError("Unsupported scaler type.")

def _cyclic_time_features(index: pd.DatetimeIndex) -> pd.DataFrame:
    """Return sin/cos time features from a DatetimeIndex."""
    df = pd.DataFrame(index=index)
    df["min_of_day"] = index.hour * 60 + index.minute
    df["day_of_week"] = index.dayofweek
    df["day_of_year"] = index.dayofyear
    # periods
    feats = {"min_of_day": 1440, "day_of_week": 7, "day_of_year": 365.25}
    for k, period in feats.items():
        df[f"{k}_sin"] = np.sin(df[k] * (2 * np.pi / period))
        df[f"{k}_cos"] = np.cos(df[k] * (2 * np.pi / period))
        df.drop(columns=[k], inplace=True)
    return df

# -----------------------------
# Main entry you’ll call from prep_dataset.py
# -----------------------------
def apply_legacy_pipeline(
    df_with_time: pd.DataFrame,
    *,
    fit: bool,
    scaler: Any | None = None,
    scaler_name: str | None = "StandardScaler",
    clip: bool = False,
    clip_method: str = "percentile",
    clip_factor: float = 0.1,
    knn_window: int = 2,
    nan_threshold: float = 0.33,
    no_drop: bool = True,
    add_val: float | None = None,
) -> Tuple[pd.DataFrame, Any | None]:
    """
    Accepts a DataFrame that HAS a 'time' column.
    Returns (processed_df_with_time, fitted_or_passed_scaler).

    Steps:
      1) set DatetimeIndex from 'time'
      2) (optional) clip outliers
      3) KNN-impute across all numeric columns
      4) scale (fit if fit=True; else transform with provided scaler)
      5) (optional) add a constant
      6) add cyclic time features
      7) restore 'time' column
    """
    if "time" not in df_with_time.columns:
        raise ValueError("apply_legacy_pipeline expects a 'time' column.")

    # 1) index
    idx = pd.to_datetime(df_with_time["time"], errors="coerce")
    X = df_with_time.drop(columns=["time"]).copy()
    X.index = pd.DatetimeIndex(idx)

    # 2) optional clip
    if clip:
        X = clip_outliers(X, method=clip_method, factor=clip_factor)

    # 3) KNN-impute
    X = handle_nans(X, threshold=nan_threshold, window=int(knn_window), no_drop=no_drop)

    # 4) scale
    sc = scaler
    if fit:
        sc = _make_scaler(scaler_name)
        if sc is not None:
            X[:] = sc.fit_transform(X.values)
    else:
        if sc is not None:
            X[:] = sc.transform(X.values)

    # 5) optional add constant
    if add_val is not None:
        X = X.add(float(add_val))

    # 6) time features
    tf = _cyclic_time_features(X.index)
    X = pd.concat([X, tf], axis=1)

    # 7) restore 'time'
    out = X.reset_index().rename(columns={"index": "time"})
    return out, sc

# (Optional utilities you might re-use later)
def subset_scaler(original_scaler, original_columns: List[str], subset_columns: List[str]):
    """Create a new scaler limited to a subset of columns."""
    if original_columns == subset_columns:
        return original_scaler
    subset_idx = [original_columns.index(c) for c in subset_columns]
    if isinstance(original_scaler, StandardScaler):
        sub = StandardScaler()
        sub.mean_  = None if original_scaler.mean_  is None else original_scaler.mean_[subset_idx]
        sub.scale_ = None if original_scaler.scale_ is None else original_scaler.scale_[subset_idx]
    elif isinstance(original_scaler, MinMaxScaler):
        sub = MinMaxScaler()
        sub.min_ = original_scaler.min_[subset_idx]
        sub.scale_ = original_scaler.scale_[subset_idx]
        sub.data_min_ = original_scaler.data_min_[subset_idx]
        sub.data_max_ = original_scaler.data_max_[subset_idx]
        sub.data_range_ = original_scaler.data_range_[subset_idx]
    elif isinstance(original_scaler, RobustScaler):
        sub = RobustScaler()
        sub.center_ = original_scaler.center_[subset_idx]
        sub.scale_  = original_scaler.scale_[subset_idx]
    elif isinstance(original_scaler, MaxAbsScaler):
        sub = MaxAbsScaler()
        sub.max_abs_ = original_scaler.max_abs_[subset_idx]
        sub.scale_   = original_scaler.scale_[subset_idx]
    else:
        raise TypeError("Unsupported scaler type for subset_scaler.")
    sub.n_features_in_ = len(subset_columns)
    sub.feature_names_in_ = np.array(subset_columns, dtype=object)
    return sub
