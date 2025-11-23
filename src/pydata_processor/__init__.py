"""Top-level package for Python Data Processor.

This package provides functionality to perform ETL (Extract, Transform, Load)
operations on CSV and JSON datasets via both a Python API and a CLI interface.

Modules
-------
processor : Core data transformation logic (cleaning, filtering, deduplication).
cli        : Command-line interface powered by Typer.

Example
-------
>>> from pydata_processor import processor
>>> df = processor.load_transform("data/data.csv")
>>> summary = processor.summary(df)
>>> print(summary)
{'rows': 5, 'columns': 4, ...}
"""

__all__ = ["processor"]
__version__ = "0.1.0"
