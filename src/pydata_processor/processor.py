from __future__ import annotations
import pathlib
import pandas as pd


def _read(path: str | pathlib.Path) -> pd.DataFrame:
    path = pathlib.Path(path)
    if path.suffix.lower() in {".csv"}:
        return pd.read_csv(path)
    if path.suffix.lower() in {".json"}:
        return pd.read_json(path, orient="records", lines=False)
    raise ValueError(f"Unsupported input format: {path.suffix}")


def _write(df: pd.DataFrame, path: str | pathlib.Path) -> None:
    path = pathlib.Path(path)
    if path.suffix.lower() in {".csv"}:
        df.to_csv(path, index=False)
        return
    if path.suffix.lower() in {".json"}:
        df.to_json(path, orient="records", indent=2)
        return
    raise ValueError(f"Unsupported output format: {path.suffix}")


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Trim whitespace and convert column names to snake_case."""
    def snake(s: str) -> str:
        s = s.strip()
        s = s.replace("-", " ").replace("/", " ")
        s = "".join(ch if ch.isalnum() else " " for ch in s)
        s = "_".join(x for x in s.lower().split() if x)
        return s
    df = df.copy()
    df.columns = [snake(c) for c in df.columns]
    return df


def drop_null_rows(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(how="all")


def dedupe(df: pd.DataFrame, subset: list[str] | None = None) -> pd.DataFrame:
    return df.drop_duplicates(subset=subset)


def filter_query(df: pd.DataFrame, query: str | None) -> pd.DataFrame:
    if not query:
        return df
    # pandas query syntax, e.g. 'amount > 100 and country == "DE"'
    return df.query(query)


def summary(df: pd.DataFrame) -> dict:
    return {
        "rows": int(len(df)),
        "columns": int(df.shape[1]),
        "nulls": {c: int(df[c].isna().sum()) for c in df.columns},
        "dtypes": {c: str(df[c].dtype) for c in df.columns},
    }


def load_transform(
    input_path: str | pathlib.Path,
    query: str | None = None,
    subset_for_dedupe: list[str] | None = None,
) -> pd.DataFrame:
    df = _read(input_path)
    df = clean_columns(df)
    df = drop_null_rows(df)
    df = dedupe(df, subset_for_dedupe)
    df = filter_query(df, query)
    return df


def process(
    input_path: str | pathlib.Path,
    output_path: str | pathlib.Path,
    query: str | None = None,
    subset_for_dedupe: list[str] | None = None,
) -> dict:
    df = load_transform(input_path, query, subset_for_dedupe)
    _write(df, output_path)
    return summary(df)
