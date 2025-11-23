"""Command-line interface for the Python Data Processor.

This module uses the `typer` library to provide an easy-to-use
CLI for performing ETL (Extract, Transform, Load) operations
on CSV and JSON files.

Available commands
------------------
summary : Print a compact summary of a dataset.
convert : Convert between CSV and JSON formats.
filter  : Clean, deduplicate, filter, and save results.

Examples
--------
# show summary of a CSV file
python -m pydata_processor.cli summary data/input.csv

# convert CSV to JSON
python -m pydata_processor.cli convert data/input.csv data/output.json

# filter and clean
python -m pydata_processor.cli filter data/input.csv data/clean.csv \
    --query 'age > 25' --subset 'id,name'
"""

from __future__ import annotations
import json
from pathlib import Path
import typer
from . import processor

app = typer.Typer(
    help="Simple ETL for CSV/JSON: clean, filter, dedupe, summarize."
)


@app.command()
def summary(input: Path):
    """Print a quick data summary.

    Loads and transforms the input dataset, then prints a JSON summary
    showing number of rows, columns, null counts, and column data types.

    Parameters
    ----------
    input : Path
        Path to the CSV or JSON file to summarize.
    """
    df = processor.load_transform(input)
    typer.echo(json.dumps(processor.summary(df), indent=2))


@app.command()
def convert(input: Path, output: Path):
    """Convert CSV<->JSON without filtering.

    Reads the input file, normalizes columns, removes empty rows,
    and writes the result to the target file (either CSV or JSON).

    Parameters
    ----------
    input : Path
        Input file path (.csv or .json).
    output : Path
        Output file path (.csv or .json).
    """
    info = processor.process(input, output)
    typer.echo(f"Saved {output}  (rows={info['rows']}, cols={info['columns']})")


@app.command()
def filter(
    input: Path,
    output: Path,
    query: str = typer.Option(
        None,
        help='pandas query, e.g. amount > 100 and country == "DE"'
    ),
    subset: str = typer.Option(
        None,
        help="comma-separated subset of columns for duplicate removal"
    ),
):
    """Clean, deduplicate, filter, and save data.

    Combines cleaning, filtering, and deduplication into one command.

    Parameters
    ----------
    input : Path
        Path to input file (.csv or .json).
    output : Path
        Path to output file (.csv or .json).
    query : str, optional
        A pandas query string used to filter rows, e.g. ``age > 30``.
    subset : str, optional
        Comma-separated column names for deduplication, e.g. ``id,name``.
    """
    subset_cols = [s.strip() for s in subset.split(",")] if subset else None
    info = processor.process(input, output, query=query, subset_for_dedupe=subset_cols)
    typer.echo(json.dumps(info, indent=2))
    typer.echo(f"Saved {output}")


if __name__ == "__main__":
    app()
