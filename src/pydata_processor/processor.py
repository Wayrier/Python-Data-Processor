"""Core data processing utilities for the Python Data Processor.

This module provides helper functions to:

- read CSV/JSON files into pandas DataFrames
- clean and normalize column names
- drop empty rows
- deduplicate records
- filter rows via a pandas-style query string
- write results back to CSV/JSON
- compute a compact summary dictionary for reporting

The high-level entry points are:

- ``load_transform``: load + clean + filter, return DataFrame
- ``process``: load + clean + filter + write, return summary dict
"""

from __future__ import annotations
import pathlib
import pandas as pd


def _read(path: str | pathlib.Path) -> pd.DataFrame:
    """Read a CSV or JSON file into a pandas DataFrame.

    Parameters
    ----------
    path:
        Path to an input file. Supported extensions:
        - ``.csv``  (read via :func:`pandas.read_csv`)
        - ``.json`` (read via :func:`pandas.read_json` with
          ``orient='records'`` and ``lines=False``)

    Returns
    -------
    pd.DataFrame
        Data loaded from the given file.

    Raises
    ------
    ValueError
        If the file extension is not supported.
    """
    path = pathlib.Path(path)
    if path.suffix.lower() in {".csv"}:
        return pd.read_csv(path)
    if path.suffix.lower() in {".json"}:
        return pd.read_json(path, orient="records", lines=False)
    raise ValueError(f"Unsupported input format: {path.suffix}")


def _write(df: pd.DataFrame, path: str | pathlib.Path) -> None:
    """Write a DataFrame to CSV or JSON.

    Parameters
    ----------
    df:
        DataFrame to be written.
    path:
        Target file path. Supported extensions:
        - ``.csv``  → ``DataFrame.to_csv(index=False)``
        - ``.json`` → ``DataFrame.to_json(orient='records', indent=2)``

    Raises
    ------
    ValueError
        If the file extension is not supported.
    """
    path = pathlib.Path(path)
    if path.suffix.lower() in {".csv"}:
        df.to_csv(path, index=False)
        return
    if path.suffix.lower() in {".json"}:
        df.to_json(path, orient="records", indent=2)
        return
    raise ValueError(f"Unsupported output format: {path.suffix}")


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names by trimming and converting to ``snake_case``.

    Steps performed:

    - trim leading/trailing whitespace
    - replace ``-`` and ``/`` with spaces
    - remove non-alphanumeric characters
    - collapse multiple spaces
    - convert to lower-case ``snake_case`` (words joined by ``_``)

    Parameters
    ----------
    df:
        Input DataFrame whose columns will be normalized.

    Returns
    -------
    pd.DataFrame
        A copy of the original DataFrame with cleaned column names.
    """

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
    """Drop rows that are completely empty (all values are NaN).

    Parameters
    ----------
    df:
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with rows removed where *all* columns are null.
    """
    return df.dropna(how="all")


def dedupe(df: pd.DataFrame, subset: list[str] | None = None) -> pd.DataFrame:
    """Remove duplicate rows from a DataFrame.

    Parameters
    ----------
    df:
        Input DataFrame.
    subset:
        Optional list of column names to consider for duplicate detection.
        If ``None``, all columns are used.

    Returns
    -------
    pd.DataFrame
        DataFrame with duplicate rows removed.
    """
    return df.drop_duplicates(subset=subset)


def filter_query(df: pd.DataFrame, query: str | None) -> pd.DataFrame:
    """Filter rows using a pandas-style query expression.

    Parameters
    ----------
    df:
        Input DataFrame.
    query:
        A query string in pandas syntax (e.g. ``'amount > 100 and country == "DE"'``).
        If ``None`` or an empty string, the original DataFrame is returned unchanged.

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame.
    """
    if not query:
        return df
    # pandas query syntax, e.g. 'amount > 100 and country == "DE"'
    return df.query(query)


def summary(df: pd.DataFrame) -> dict:
    """Compute a compact summary of a DataFrame.

    The summary contains:

    - total number of rows
    - total number of columns
    - per-column count of null values
    - per-column dtype as string

    Parameters
    ----------
    df:
        Input DataFrame.

    Returns
    -------
    dict
        A JSON-serializable dictionary with keys ``rows``, ``columns``,
        ``nulls`` and ``dtypes``.
    """
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
    """Load, clean, deduplicate and optionally filter a dataset.

    This is the main in-memory processing pipeline:

    1. read CSV/JSON from ``input_path`` (:func:`_read`)
    2. normalize column names (:func:`clean_columns`)
    3. drop fully-empty rows (:func:`drop_null_rows`)
    4. deduplicate rows (:func:`dedupe`)
    5. apply an optional query (:func:`filter_query`)

    Parameters
    ----------
    input_path:
        Path to the input file (CSV or JSON).
    query:
        Optional pandas-style query string applied after cleaning/deduplication.
    subset_for_dedupe:
        Optional list of column names for duplicate detection.

    Returns
    -------
    pd.DataFrame
        The fully processed DataFrame.
    """
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
    """End-to-end processing pipeline: load, transform and write to disk.

    High-level convenience function used by the CLI:

    - reads the input file
    - cleans columns and drops empty rows
    - deduplicates records
    - applies an optional filter
    - writes the result to ``output_path``
    - returns a summary dict for logging/CLI output

    Parameters
    ----------
    input_path:
        Path to the input CSV/JSON file.
    output_path:
        Path where the processed data should be written (CSV or JSON).
    query:
        Optional pandas-style query string for row filtering.
    subset_for_dedupe:
        Optional list of column names used for duplicate detection.

    Returns
    -------
    dict
        A summary dictionary as returned by :func:`summary`.
    """
    df = load_transform(input_path, query, subset_for_dedupe)
    _write(df, output_path)
    return summary(df)
