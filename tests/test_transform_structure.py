def test_transform_returns_all_tables(tables):
    expected = {"team", "city", "stadium", "edition", "match"}
    assert set(tables.keys()) == expected


def test_dimensions_have_id_columns(tables):
    assert "id_team" in tables["team"].columns
    assert "id_city" in tables["city"].columns
    assert "id_stadium" in tables["stadium"].columns
    assert "id_edition" in tables["edition"].columns
