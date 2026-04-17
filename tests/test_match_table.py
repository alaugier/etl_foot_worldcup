def test_match_table_columns(tables):
    match_df = tables["match"]

    expected_cols = {
        "date", "round", "home_result", "away_result", "result",
        "id_home_team", "id_away_team", "id_stadium", "id_edition"
    }

    assert expected_cols.issubset(match_df.columns)


def test_match_table_no_missing_ids(tables):
    match_df = tables["match"]

    for col in ["id_home_team", "id_away_team", "id_stadium", "id_edition"]:
        assert match_df[col].notna().all()


def test_match_count_consistency(df_raw, tables):
    match_df = tables["match"]
    assert len(match_df) == 964