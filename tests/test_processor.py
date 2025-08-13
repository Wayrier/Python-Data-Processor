from pathlib import Path
import pandas as pd
from src.pydata_processor import processor


def test_clean_and_summary(tmp_path: Path):
    df = pd.DataFrame(
        {" Name ": ["Alice", "Bob", None], "Amount (€)": [100, 100, None]}
    )
    p = tmp_path / "in.csv"
    df.to_csv(p, index=False)

    out = tmp_path / "out.csv"
    info = processor.process(p, out, query="amount_e > 50", subset_for_dedupe=["name"])

    assert info["rows"] == 2
    assert "name" in info["dtypes"]
    assert out.exists()
