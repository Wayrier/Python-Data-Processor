# Python Data Processor

[![Python CI](https://github.com/Wayrier/Python-Data-Processor/actions/workflows/python-ci.yml/badge.svg)](https://github.com/Wayrier/Python-Data-Processor/actions/workflows/python-ci.yml)

ETL-style utility that loads CSV/JSON, cleans columns, drops empty rows, de-duplicates,
optionally filters with a pandas `query`, and writes CSV/JSON. Includes tests and a CLI.

## 📘 Projektdokumentation

Die komplette Dokumentation befindet sich im Ordner **Docs**:

1. [01_overview.ipynb](./Docs/01_overview.ipynb) – Überblick & Architektur  
2. [02_setup.ipynb](./Docs/02_setup.ipynb) – Installation & Umgebung  
3. [03_usage.ipynb](./Docs/03_usage.ipynb) – Beispiele & Anwendung  
4. [04_testing.ipynb](./Docs/04_testing.ipynb) – Tests & Qualitätssicherung  
5. [05_future_work.ipynb](./Docs/05_future_work.ipynb) – Roadmap & Erweiterungen
6. [06_code_overview.ipynb](./Docs/06_code_overview.ipynb) – Übersicht über den Code, Funktionen und CLI-Struktur


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
