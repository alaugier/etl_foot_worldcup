def test_dimension_ids_are_unique(tables):
    for dim in ["team", "city", "stadium", "edition"]:
        df_dim = tables[dim]
        id_col = [c for c in df_dim.columns if c.startswith("id_")][0]
        assert df_dim[id_col].is_unique
