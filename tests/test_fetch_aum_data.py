"""Tests for the scheme-wise AUM workflow entrypoint."""

from argparse import Namespace

import pandas as pd

from scripts import fetch_aum_data


def _sample_aum() -> pd.DataFrame:
    """Return one valid row for exercising the publish path."""
    return pd.DataFrame(
        [
            {
                "scheme_code": "1001",
                "scheme_name": "Example Fund",
                "mf_name": "Example AMC",
                "scheme_type": "Open Ended",
                "aum_excl_fof": 100.0,
                "aum_fof_domestic": 0.0,
                "financial_year": "April 2025 - March 2026",
                "period": "January - March 2026",
            }
        ]
    )


def test_main_fails_when_r2_upload_fails(monkeypatch, tmp_path) -> None:
    """An unavailable R2 handoff must prevent downstream dispatch."""
    monkeypatch.setattr(
        fetch_aum_data,
        "parse_args",
        lambda: Namespace(years=5, fy=None, period=None),
    )
    monkeypatch.setattr(
        fetch_aum_data,
        "fetch_all_aum_data",
        lambda _years: _sample_aum(),
    )
    monkeypatch.setattr(
        fetch_aum_data.Paths,
        "AUM_SCHEMEWISE",
        tmp_path / "aum_schemewise.parquet",
    )
    monkeypatch.setattr(
        fetch_aum_data.Paths,
        "create_directories",
        lambda: None,
    )
    monkeypatch.setattr(
        pd.DataFrame,
        "to_parquet",
        lambda self, *args, **kwargs: None,
    )

    class FailingR2:
        """R2 stand-in that fails before an object can be published."""

        def setup_connection(self):
            raise RuntimeError("R2 unavailable")

    monkeypatch.setattr(fetch_aum_data, "R2", FailingR2)

    assert fetch_aum_data.main() is False
