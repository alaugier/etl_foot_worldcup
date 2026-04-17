def test_unknown_stadiums_are_unique_per_edition(tables):
    stadium_df = tables["stadium"]
    unknowns = stadium_df[stadium_df["stadium_name"].str.contains("UNKNOWN")]
    assert unknowns["stadium_name"].is_unique


def test_montevideo_only_in_1930(tables):
    stadium_df = tables["stadium"]
    city_df = tables["city"]

    id_montevideo = city_df.loc[
        city_df["city_name"] == "Montevideo", "id_city"
    ].iloc[0]

    stades_montevideo = stadium_df[stadium_df["id_city"] == id_montevideo]

    # Tous les stades mappés à Montevideo doivent être ceux de 1930
    assert all("1930" in name for name in stades_montevideo["stadium_name"])
