# Python Data Processor

[![Python CI](https://github.com/Wayrier/Python-Data-Processor/actions/workflows/python-ci.yml/badge.svg)](https://github.com/Wayrier/Python-Data-Processor/actions/workflows/python-ci.yml)

A small ETL-style command-line utility for CSV and JSON files. It loads data, normalizes column names, drops empty rows, removes duplicates, optionally filters rows with a pandas query, and writes the result back to CSV or JSON.

## Features

- CSV/JSON input and CSV/JSON output
- Column cleanup to `snake_case`
- Removal of fully empty rows
- Optional duplicate removal by selected columns
- Filtering via `pandas.DataFrame.query`
- CLI commands: `summary`, `convert`, `filter`
- Automated tests with pytest and GitHub Actions

## Tech Stack

- Python 3.11+
- pandas
- Typer
- pytest
- GitHub Actions

## Project Documentation

Additional documentation is available in the `Docs` folder:

1. [Overview](./Docs/01_overview.ipynb)
2. [Setup](./Docs/02_setup.ipynb)
3. [Usage](./Docs/03_usage.ipynb)
4. [Testing](./Docs/04_testing.ipynb)
5. [Future Work](./Docs/05_future_work.ipynb)
6. [Code Overview](./Docs/06_code_overview.ipynb)

## Quickstart

Create a virtual environment and install dependencies:

```powershell
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
$env:PYTHONPATH = "src"
```

Run the CLI:

```powershell
python -m pydata_processor.cli summary data/sample.csv
python -m pydata_processor.cli convert data/sample.csv out.json
python -m pydata_processor.cli filter data/sample.csv out.csv --query "amount > 100" --subset "name"
```

For macOS/Linux shells, use this instead of the PowerShell `PYTHONPATH` command:

```bash
export PYTHONPATH=src
```

## Tests

```bash
python -m pytest -q
```

The GitHub Actions workflow runs the test suite on Python 3.11 and 3.12.

## Example Data

The repository includes `data/sample.csv` for quick local testing.
