# Python Data Processor

[![Python CI](https://github.com/Wayrier/Python-Data-Processor/actions/workflows/python-ci.yml/badge.svg)](https://github.com/Wayrier/Python-Data-Processor/actions/workflows/python-ci.yml)

ETL-style utility that loads CSV/JSON, cleans columns, drops empty rows, de-duplicates,
optionally filters with a pandas `query`, and writes CSV/JSON. Includes tests and a CLI.

### 📘 Projektdokumentation
➡️ Siehe: [project_documentation.ipynb](./project_documentation.ipynb)

## Features
- Input: **CSV/JSON** → Output: **CSV/JSON**
- Cleans column names to `snake_case`
- Drops fully-empty rows
- Optional de-duplication (subset of columns)
- Filtering via `pandas.DataFrame.query`
- CLI with commands: `summary`, `convert`, `filter`

## Quickstart
```bash
# Windows (PowerShell)
py -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt

# Summary
python -m src.pydata_processor.cli summary data/sample.csv

# Convert CSV -> JSON
python -m src.pydata_processor.cli convert data/sample.csv out.json

# Filter & save (remove dups by Name; keep Amount > 100)
python -m src.pydata_processor.cli filter data/sample.csv out.csv --query "amount > 100" --subset "name"
