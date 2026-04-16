# dbt + DuckDB Pipeline

Documentation du pipeline de transformation dbt avec DuckDB pour le projet `etl_foot_worldcup`.

---

## Vue d'ensemble

```
seeds/ (CSV bruts)
    ↓  dbt seed
DuckDB (tables de staging)
    ↓  dbt run
DuckDB (star schema)
    ↓  dbt test
21 tests de qualité
```

Les données sources (3 fichiers CSV dans `seeds/`) sont chargées dans DuckDB via `dbt seed`, puis transformées en schéma en étoile par les 5 modèles SQL dans `models/`.

---

## Sources de données (`seeds/`)

| Fichier seed | Source d'origine | Couverture |
|---|---|---|
| `matches_19302010.csv` | `data_raw/matches_19302010.csv` | Coupes du Monde 1930–2010 (finales + qualifications) |
| `world_cup_matches_2014.csv` | `data_raw/WorldCupMatches2014.csv` | Coupe du Monde 2014 (Brésil) |
| `worldcup_2022.csv` | `data_raw/worldcup_2022.json` (aplati) | Coupe du Monde 2022 (Qatar) |

### Configuration des seeds (`seeds/schema.yml`)

- `matches_19302010` : délimiteur `,`
- `world_cup_matches_2014` : délimiteur `;`, colonnes de buts castées en `integer`
- `worldcup_2022` : délimiteur `,`, `home_result` / `away_result` castés en `integer`

---

## Modèles (`models/`)

### Schéma en étoile

```
dim_city ◄──── dim_stadium ◄──┐
                               fct_match
dim_team (home) ◄─────────────┤
dim_team (away) ◄─────────────┤
dim_edition ◄─────────────────┘
```

Tous les modèles sont matérialisés en `table` (voir `dbt_project.yml`).

---

### `dim_city`

**Description** : Villes hôtes distinctes sur toutes les éditions (1930–2022).

| Colonne | Type | Description |
|---|---|---|
| `id_city` | integer | Clé de substitution (surrogate key) |
| `city_name` | varchar | Nom de la ville |

**Logique** : union des 3 sources — champ `venue` (1930–2010), colonne `City` (2014), extraction regex depuis `ground` (2022 : `"Stade, Ville"` → `"Ville"`).

---

### `dim_stadium`

**Description** : Stades distincts avec FK vers `dim_city`.

| Colonne | Type | Description |
|---|---|---|
| `id_stadium` | integer | Clé de substitution |
| `stadium_name` | varchar | Nom du stade |
| `id_city` | integer | FK → `dim_city.id_city` |

**Logique** : même union 3 sources que `dim_city`, déduplication sur `stadium_name`, jointure vers `dim_city`.

---

### `dim_team`

**Description** : Toutes les équipes distinctes sur toutes les éditions.

| Colonne | Type | Description |
|---|---|---|
| `id_team` | integer | Clé de substitution |
| `team_name` | varchar | Nom de l'équipe |

**Logique** : union des noms `team1` / `team2` des 3 sources. Pour 1930–2010, suppression des traductions entre parenthèses (`"Brazil (Brasil)"` → `"Brazil"`).

---

### `dim_edition`

**Description** : Une ligne par tournoi FIFA.

| Colonne | Type | Description |
|---|---|---|
| `id_edition` | integer | Clé de substitution |
| `edition_name` | varchar | Pays hôte |
| `year` | integer | Année du tournoi |

**Logique** : extraction depuis le champ `edition` (ex. `"1930-URUGUAY"`) pour 1930–2010, puis `UNION ALL` avec les lignes fixes 2014/2022.

---

### `fct_match`

**Description** : Une ligne par match (finales uniquement, sans qualifications), toutes éditions 1930–2022.

| Colonne | Type | Description |
|---|---|---|
| `id_match` | integer | Clé de substitution |
| `date` | date | Date du match |
| `round` | varchar | Phase (Group stage, Round of 16, …) |
| `home_result` | integer | Buts équipe domicile |
| `away_result` | integer | Buts équipe extérieur |
| `result` | varchar | `'home'` / `'away'` / `'draw'` |
| `id_home_team` | integer | FK → `dim_team.id_team` |
| `id_away_team` | integer | FK → `dim_team.id_team` |
| `id_stadium` | integer | FK → `dim_stadium.id_stadium` |
| `id_edition` | integer | FK → `dim_edition.id_edition` |

**Normalisation des phases (`round`)** :

| Valeur brute | Valeur normalisée |
|---|---|
| `GROUP_STAGE`, `FIRST`, `FINAL_ROUND` | `Group stage` |
| `1/8_FINAL` | `Round of 16` |
| `1/4_FINAL`, `QUARTERFINAL_STAGE` | `Quarter-final` |
| `1/2_FINAL`, `SEMIFINAL_STAGE` | `Semi-final` |
| `_FINAL` | `Final` |
| `PLACES_3&4` | `Third place` |

---

## Tests de qualité (`models/schema.yml`)

21 tests exécutés via `dbt test` :

| Modèle | Test |
|---|---|
| `dim_city` | `unique` + `not_null` sur `id_city`, `not_null` sur `city_name` |
| `dim_stadium` | `unique` + `not_null` sur `id_stadium`, `not_null` sur `stadium_name` |
| `dim_team` | `unique` + `not_null` sur `id_team`, `not_null` sur `team_name` |
| `dim_edition` | `unique` + `not_null` sur `id_edition`, `unique` + `not_null` sur `year`, `not_null` sur `edition_name` |
| `fct_match` | `unique` + `not_null` sur `id_match`, `not_null` sur `result`, `accepted_values` (`home`/`away`/`draw`), `not_null` sur `id_home_team`, `id_away_team`, `id_edition` |

---

## Configuration

### `dbt_project.yml`

```yaml
name: foot_etl
profile: foot_etl
model-paths: ["models"]
seed-paths:  ["seeds"]

models:
  foot_etl:
    +materialized: table
```

### `profiles.yml`

```yaml
foot_etl:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('DBT_DUCKDB_PATH', 'foot_etl.duckdb') }}"
      threads: 4
```

Le chemin du fichier DuckDB peut être surchargé via la variable d'environnement `DBT_DUCKDB_PATH`.

---

## Lancer le pipeline

```bash
# 1. Installer les dépendances
uv sync

# 2. Charger les seeds dans DuckDB
uv run dbt seed

# 3. Construire le schéma en étoile
uv run dbt run

# 4. Vérifier la qualité des données
uv run dbt test
```

Résultat attendu : `PASS=3` (seed), `PASS=5` (run), `PASS=21` (test).

Le fichier `foot_etl.duckdb` est généré à la racine du projet et est lu par le notebook `notebooks/kpi_duckdb.ipynb`.
