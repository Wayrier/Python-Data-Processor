from __future__ import annotations
import json
from pathlib import Path
import typer
from . import processor

app = typer.Typer(help="Simple ETL for CSV/JSON: clean, filter, dedupe, summarize.")


@app.command()
def summary(input: Path):
    """Print a quick data summary."""
    df = processor.load_transform(input)
    typer.echo(json.dumps(processor.summary(df), indent=2))


@app.command()
def convert(input: Path, output: Path):
    """Convert CSV<->JSON without filtering."""
    info = processor.process(input, output)
    typer.echo(f"Saved {output}  (rows={info['rows']}, cols={info['columns']})")


@app.command()
def filter(
    input: Path,
    output: Path,
    query: str = typer.Option(None, help='pandas query, e.g. amount > 100 and country == "DE"'),
    subset: str = typer.Option(
        None, help="comma-separated subset of columns for duplicate removal"
    ),
):
    """Clean + dedupe + filter and save."""
    subset_cols = [s.strip() for s in subset.split(",")] if subset else None
    info = processor.process(input, output, query=query, subset_for_dedupe=subset_cols)
    typer.echo(json.dumps(info, indent=2))
    typer.echo(f"Saved {output}")


if __name__ == "__main__":
    app()
