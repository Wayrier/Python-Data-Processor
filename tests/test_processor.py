import json

import pytest

from pydata_processor import processor


def test_load_transform_cleans_deduplicates_and_filters(tmp_path):
    input_file = tmp_path / "sample.csv"
    input_file.write_text(
        "Name,Country,Amount\nAlice,DE,120\nBob,DE,90\nAlice,DE,120\n,,\n",
        encoding="utf-8",
    )

    df = processor.load_transform(
        input_file,
        query="amount > 100",
        subset_for_dedupe=["name"],
    )

    assert list(df.columns) == ["name", "country", "amount"]
    assert len(df) == 1
    assert df.iloc[0]["name"] == "Alice"
    assert df.iloc[0]["amount"] == 120


def test_process_writes_json_and_returns_summary(tmp_path):
    input_file = tmp_path / "sample.csv"
    output_file = tmp_path / "out.json"
    input_file.write_text("Name,Amount\nAlice,120\nBob,90\n", encoding="utf-8")

    result = processor.process(input_file, output_file)

    assert result["rows"] == 2
    assert result["columns"] == 2
    assert output_file.exists()

    data = json.loads(output_file.read_text(encoding="utf-8"))
    assert data == [
        {"name": "Alice", "amount": 120},
        {"name": "Bob", "amount": 90},
    ]


def test_unsupported_input_format_raises(tmp_path):
    input_file = tmp_path / "sample.txt"
    input_file.write_text("Name,Amount\nAlice,120\n", encoding="utf-8")

    with pytest.raises(ValueError, match="Unsupported input format"):
        processor.load_transform(input_file)
