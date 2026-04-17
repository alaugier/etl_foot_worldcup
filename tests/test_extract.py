import pandas as pd


def test_extract_matches_loads_dataframe(df_raw):
    assert isinstance(df_raw, pd.DataFrame)
    assert len(df_raw) > 0


def test_extract_matches_columns(df_raw):
    expected_cols = {
        "home_team", "away_team", "home_result", "away_result",
        "result", "date", "round", "city", "stadium", "edition"
    }
    assert expected_cols.issubset(df_raw.columns)
