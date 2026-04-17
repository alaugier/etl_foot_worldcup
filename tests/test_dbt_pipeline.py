"""
Tests pytest pour le pipeline dbt + DuckDB.

Ces tests complètent les tests dbt natifs (uniqueness, not_null, accepted_values)
en vérifiant la logique métier, la cohérence inter-tables et les comptages attendus.

Prérequis : avoir exécuté `dbt seed && dbt run` avant de lancer pytest.
"""

import pytest
import duckdb

DB_PATH = "foot_etl.duckdb"


@pytest.fixture(scope="module")
def con():
    """Connexion DuckDB partagée pour tous les tests du module."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    yield conn
    conn.close()


# ── Structure ─────────────────────────────────────────────────────────────────

class TestSchema:
    """Vérifie que les tables existent et ont les colonnes attendues."""

    def test_all_tables_exist(self, con):
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        expected = {"fct_match", "dim_team", "dim_city", "dim_stadium", "dim_edition"}
        assert expected.issubset(tables)

    def test_fct_match_columns(self, con):
        cols = {row[0] for row in con.execute("DESCRIBE fct_match").fetchall()}
        expected = {
            "id_match", "date", "round", "home_result", "away_result",
            "result", "id_home_team", "id_away_team", "id_stadium", "id_edition",
        }
        assert expected.issubset(cols)

    def test_dim_edition_columns(self, con):
        cols = {row[0] for row in con.execute("DESCRIBE dim_edition").fetchall()}
        assert {"id_edition", "year", "edition_name"}.issubset(cols)

    def test_dim_team_columns(self, con):
        cols = {row[0] for row in con.execute("DESCRIBE dim_team").fetchall()}
        assert {"id_team", "team_name"}.issubset(cols)


# ── Comptages ─────────────────────────────────────────────────────────────────

class TestCounts:
    """Vérifie les volumes de données attendus."""

    def test_total_matches(self, con):
        count = con.execute("SELECT count(*) FROM fct_match").fetchone()[0]
        # 1930-2022 : 22 éditions, ~964 matchs
        assert count >= 900, f"Trop peu de matchs : {count}"

    def test_editions_count(self, con):
        count = con.execute("SELECT count(*) FROM dim_edition").fetchone()[0]
        # De 1930 à 2022 : 22 éditions (pas de tournoi en 1942 et 1946)
        assert count == 22

    def test_matches_per_edition(self, con):
        """Les éditions récentes (1998+) doivent avoir exactement 64 matchs."""
        rows = con.execute("""
            SELECT e.year, count(*) as n
            FROM fct_match m
            JOIN dim_edition e ON e.id_edition = m.id_edition
            WHERE e.year >= 1998
            GROUP BY e.year
        """).fetchall()
        for year, n in rows:
            assert n == 64, f"Edition {year} : {n} matchs (attendu 64)"

    def test_2018_present(self, con):
        """La Coupe du Monde 2018 doit être présente avec 64 matchs."""
        count = con.execute("""
            SELECT count(*) FROM fct_match m
            JOIN dim_edition e ON e.id_edition = m.id_edition
            WHERE e.year = 2018
        """).fetchone()[0]
        assert count == 64

    def test_no_orphan_matches(self, con):
        """Tous les matchs doivent avoir leurs FK résolues (pas de NULL)."""
        row = con.execute("""
            SELECT count(*) FROM fct_match
            WHERE id_home_team IS NULL
               OR id_away_team IS NULL
               OR id_stadium   IS NULL
               OR id_edition   IS NULL
        """).fetchone()[0]
        assert row == 0, f"{row} matchs avec des FK nulles"


# ── Logique métier ────────────────────────────────────────────────────────────

class TestBusinessLogic:
    """Vérifie des règles métier qui vont au-delà des tests dbt natifs."""

    def test_result_consistency(self, con):
        """Le champ result doit être cohérent avec home_result et away_result."""
        inconsistent = con.execute("""
            SELECT count(*) FROM fct_match
            WHERE (home_result > away_result AND result != 'home')
               OR (away_result > home_result AND result != 'away')
               OR (home_result = away_result AND result != 'draw')
        """).fetchone()[0]
        assert inconsistent == 0, f"{inconsistent} matchs avec result incohérent"

    def test_goals_non_negative(self, con):
        """Les scores ne peuvent pas être négatifs."""
        count = con.execute("""
            SELECT count(*) FROM fct_match
            WHERE home_result < 0 OR away_result < 0
        """).fetchone()[0]
        assert count == 0

    def test_all_editions_have_a_final(self, con):
        """Chaque édition doit avoir exactement un match de type 'Final'."""
        rows = con.execute("""
            SELECT e.year, count(*) as n
            FROM fct_match m
            JOIN dim_edition e ON e.id_edition = m.id_edition
            WHERE m.round = 'Final'
            GROUP BY e.year
        """).fetchall()
        for year, n in rows:
            assert n == 1, f"Edition {year} : {n} finales (attendu 1)"

    def test_known_editions_present(self, con):
        """Vérifie que des éditions clés sont bien présentes."""
        years = {row[0] for row in con.execute("SELECT year FROM dim_edition").fetchall()}
        for expected_year in [1930, 1966, 1998, 2014, 2018, 2022]:
            assert expected_year in years, f"Edition {expected_year} manquante"

    def test_known_teams_present(self, con):
        """Vérifie que les grandes nations du football sont présentes."""
        teams = {row[0] for row in con.execute("SELECT team_name FROM dim_team").fetchall()}
        for team in ["Brazil", "Germany", "France", "Argentina"]:
            assert team in teams, f"Équipe '{team}' manquante"

    def test_home_away_teams_differ(self, con):
        """Une équipe ne peut pas jouer contre elle-même."""
        count = con.execute("""
            SELECT count(*) FROM fct_match
            WHERE id_home_team = id_away_team
        """).fetchone()[0]
        assert count == 0


# ── Intégrité référentielle ───────────────────────────────────────────────────

class TestReferentialIntegrity:
    """Vérifie les jointures entre fct_match et les dimensions."""

    def test_all_home_teams_in_dim_team(self, con):
        missing = con.execute("""
            SELECT count(*) FROM fct_match m
            LEFT JOIN dim_team t ON t.id_team = m.id_home_team
            WHERE t.id_team IS NULL
        """).fetchone()[0]
        assert missing == 0

    def test_all_stadiums_have_a_city(self, con):
        missing = con.execute("""
            SELECT count(*) FROM dim_stadium
            WHERE id_city IS NULL
        """).fetchone()[0]
        assert missing == 0

    def test_all_editions_in_fct_match(self, con):
        """Chaque édition de dim_edition doit apparaître dans fct_match."""
        missing = con.execute("""
            SELECT count(*) FROM dim_edition e
            LEFT JOIN fct_match m ON m.id_edition = e.id_edition
            WHERE m.id_edition IS NULL
        """).fetchone()[0]
        assert missing == 0
