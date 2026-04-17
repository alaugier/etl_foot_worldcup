---

# 🟩 KPI 1 — Nombre de matchs par édition  
*(permet de vérifier la cohérence du dataset et l’évolution du format de la compétition)*

```sql
SELECT 
    edition,
    COUNT(*) AS nb_matches
FROM match_final
GROUP BY edition
ORDER BY edition;
```

+-------------------+-------------+
| edition           | total_goals |
+-------------------+-------------+
| 1930-URUGUAY      |          70 |
| 1934-ITALY        |          70 |
| 1938-FRANCE       |          84 |
| 1950-BRAZIL       |          88 |
| 1954-SWITZERLAND  |         140 |
| 1958-SWEDEN       |         126 |
| 1962-CHILE        |          89 |
| 1966-ENGLAND      |          89 |
| 1970-MEXICO       |          95 |
| 1974-FRG          |          97 |
| 1978-ARGENTINA    |         102 |
| 1982-SPAIN        |         146 |
| 1986-MEXICO       |         132 |
| 1990-ITALY        |         115 |
| 1994-USA          |         141 |
| 1998-FRANCE       |         171 |
| 2002-KOREA/JAPAN  |         161 |
| 2006-GERMANY      |         147 |
| 2010-SOUTH AFRICA |         145 |
| 2014-BRAZIL       |         171 |
| 2018-RUSSIA       |         169 |
| 2022-QATAR        |         168 |
+-------------------+-------------+
22 rows in set (0.02 sec)

**Interprétation :**  
On voit une montée progressive du nombre de matchs (1930 → 2022), avec des paliers correspondant aux changements de format FIFA.

---

# 🟩 KPI 2 — Nombre total de buts par édition  
*(mesure l’intensité offensive des tournois)*

```sql
SELECT 
    edition,
    SUM(home_result + away_result) AS total_goals
FROM match_final
GROUP BY edition
ORDER BY edition;
```

+-------------------+-------------+
| edition           | total_goals |
+-------------------+-------------+
| 1930-URUGUAY      |          70 |
| 1934-ITALY        |          70 |
| 1938-FRANCE       |          84 |
| 1950-BRAZIL       |          88 |
| 1954-SWITZERLAND  |         140 |
| 1958-SWEDEN       |         126 |
| 1962-CHILE        |          89 |
| 1966-ENGLAND      |          89 |
| 1970-MEXICO       |          95 |
| 1974-FRG          |          97 |
| 1978-ARGENTINA    |         102 |
| 1982-SPAIN        |         146 |
| 1986-MEXICO       |         132 |
| 1990-ITALY        |         115 |
| 1994-USA          |         141 |
| 1998-FRANCE       |         171 |
| 2002-KOREA/JAPAN  |         161 |
| 2006-GERMANY      |         147 |
| 2010-SOUTH AFRICA |         145 |
| 2014-BRAZIL       |         171 |
| 2018-RUSSIA       |         169 |
| 2022-QATAR        |         168 |
+-------------------+-------------+
22 rows in set (0.00 sec)

**Interprétation :**  
Certaines éditions sont très prolifiques (1954), d’autres beaucoup plus fermées (1990).

---

# 🟩 KPI 3 — Moyenne de buts par match par édition  
*(KPI très utilisé en analyse sportive)*

```sql
SELECT 
    edition,
    ROUND(AVG(home_result + away_result), 2) AS avg_goals_per_match
FROM match_final
GROUP BY edition
ORDER BY edition;
```

+-------------------+---------------------+
| edition           | avg_goals_per_match |
+-------------------+---------------------+
| 1930-URUGUAY      |                3.89 |
| 1934-ITALY        |                4.12 |
| 1938-FRANCE       |                4.67 |
| 1950-BRAZIL       |                4.00 |
| 1954-SWITZERLAND  |                5.38 |
| 1958-SWEDEN       |                3.60 |
| 1962-CHILE        |                2.78 |
| 1966-ENGLAND      |                2.78 |
| 1970-MEXICO       |                2.97 |
| 1974-FRG          |                2.55 |
| 1978-ARGENTINA    |                2.68 |
| 1982-SPAIN        |                2.81 |
| 1986-MEXICO       |                2.54 |
| 1990-ITALY        |                2.21 |
| 1994-USA          |                2.71 |
| 1998-FRANCE       |                2.67 |
| 2002-KOREA/JAPAN  |                2.52 |
| 2006-GERMANY      |                2.30 |
| 2010-SOUTH AFRICA |                2.27 |
| 2014-BRAZIL       |                2.67 |
| 2018-RUSSIA       |                2.64 |
| 2022-QATAR        |                2.63 |
+-------------------+---------------------+
22 rows in set (0.01 sec)

**Interprétation attendue :**  
La moyenne oscille entre 2 et 3 buts/match selon les époques.

---

# 🟩 KPI 4 — Répartition des résultats (home win / away win / draw)  
*(utile pour un futur modèle de prédiction)*

```sql
SELECT 
    result,
    COUNT(*) AS nb_matches,
    ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM match_final), 2) AS pct
FROM match_final
GROUP BY result
ORDER BY nb_matches DESC;
```

+------------------------+------------+-------+
| result                 | nb_matches | pct   |
+------------------------+------------+-------+
| draw                   |        214 | 22.20 |
| Brazil                 |         76 |  7.88 |
| Germany                |         68 |  7.05 |
| Argentina              |         47 |  4.88 |
| Italy                  |         45 |  4.67 |
| France                 |         39 |  4.05 |
| England                |         32 |  3.32 |
| Spain                  |         31 |  3.22 |
| Netherlands            |         30 |  3.11 |
| Uruguay                |         25 |  2.59 |
| Belgium                |         21 |  2.18 |
| Russia                 |         19 |  1.97 |
| Sweden                 |         19 |  1.97 |
| Serbia                 |         18 |  1.87 |
| Mexico                 |         17 |  1.76 |
| Poland                 |         17 |  1.76 |
| Portugal               |         17 |  1.76 |
| Hungary                |         15 |  1.56 |
| Switzerland            |         14 |  1.45 |
| Croatia                |         13 |  1.35 |
| Czech Republic         |         12 |  1.24 |
| Austria                |         12 |  1.24 |
| Chile                  |         11 |  1.14 |
| Colombia               |          9 |  0.93 |
| Denmark                |          9 |  0.93 |
| Romania                |          8 |  0.83 |
| United States          |          8 |  0.83 |
| Paraguay               |          7 |  0.73 |
| South Korea            |          7 |  0.73 |
| Japan                  |          7 |  0.73 |
| Costa Rica             |          6 |  0.62 |
| Nigeria                |          6 |  0.62 |
| Morocco                |          5 |  0.52 |
| Turkey                 |          5 |  0.52 |
| Ghana                  |          5 |  0.52 |
| Senegal                |          5 |  0.52 |
| Ecuador                |          5 |  0.52 |
| Peru                   |          5 |  0.52 |
| Cameroon               |          5 |  0.52 |
| Australia              |          4 |  0.41 |
| Scotland               |          4 |  0.41 |
| Saudi Arabia           |          4 |  0.41 |
| Iran                   |          3 |  0.31 |
| Bulgaria               |          3 |  0.31 |
| Northern Ireland       |          3 |  0.31 |
| Algeria                |          3 |  0.31 |
| Tunisia                |          3 |  0.31 |
| Ivory Coast            |          3 |  0.31 |
| Greece                 |          2 |  0.21 |
| Ukraine                |          2 |  0.21 |
| GDR                    |          2 |  0.21 |
| South Africa           |          2 |  0.21 |
| Ireland                |          2 |  0.21 |
| Norway                 |          2 |  0.21 |
| Slovakia               |          1 |  0.10 |
| Slovenia               |          1 |  0.10 |
| Jamaica                |          1 |  0.10 |
| USA                    |          1 |  0.10 |
| Bosnia and Herzegovina |          1 |  0.10 |
| North Korea            |          1 |  0.10 |
| Wales                  |          1 |  0.10 |
| Cuba                   |          1 |  0.10 |
+------------------------+------------+-------+
62 rows in set (0.01 sec)

**Interprétation attendue :**  
Les victoires du pays hôte (home_team) ne sont pas forcément majoritaires, car “home_team” n’est pas “équipe qui joue à domicile”.

---

# 🟩 KPI 5 — Équipe la plus victorieuse en Coupe du Monde  
*(KPI classique, très attendu)*

```sql
SELECT 
    result AS winning_team,
    COUNT(*) AS wins
FROM match_final
WHERE result <> 'draw'
GROUP BY winning_team
ORDER BY wins DESC
LIMIT 10;
```
+--------------+------+
| winning_team | wins |
+--------------+------+
| Brazil       |   76 |
| Germany      |   68 |
| Argentina    |   47 |
| Italy        |   45 |
| France       |   39 |
| England      |   32 |
| Spain        |   31 |
| Netherlands  |   30 |
| Uruguay      |   25 |
| Belgium      |   21 |
+--------------+------+
10 rows in set (0.01 sec)

**Interprétation attendue :**  
Les grandes nations (Brésil, Allemagne…) ressortent naturellement.

---

# 🟩 KPI 6 — Ville ayant accueilli le plus de matchs  
*(montre l’importance des villes hôtes dans l’histoire du tournoi)*

```sql
SELECT 
    city,
    COUNT(*) AS nb_matches
FROM match_final
GROUP BY city
ORDER BY nb_matches DESC
LIMIT 10;
```
+------------------+------------+
| city             | nb_matches |
+------------------+------------+
| Montevideo       |        772 |
| Al Rayyan        |         22 |
| Doha             |         15 |
| Moscow           |         12 |
| Lusail           |         11 |
| Al Khor          |          9 |
| Al Wakrah        |          7 |
| Saint Petersburg |          7 |
| Rio de Janeiro   |          7 |
| Brasilia         |          7 |
+------------------+------------+
10 rows in set (0.00 sec)

**Interprétation :**  
Des villes comme Mexico City, Rio, Berlin, Doha peuvent apparaître selon les éditions.

---

# 🟩 KPI 7 — Nombre de matchs par stade  
*(utile pour analyser la répartition géographique)*

```sql
SELECT 
    s.stadium_name,
    c.city_name,
    COUNT(*) AS nb_matches
FROM `match` m
JOIN stadium s ON m.id_stadium = s.id_stadium
LEFT JOIN city c ON s.id_city = c.id_city
GROUP BY s.stadium_name, c.city_name
ORDER BY nb_matches DESC
LIMIT 10;
```
+---------------------------+-------------------+------------+
| stadium_name              | city_name         | nb_matches |
+---------------------------+-------------------+------------+
| UNKNOWN_2006-GERMANY      | MA 1/4nchen       |         64 |
| UNKNOWN_1998-FRANCE       | Paris             |         64 |
| UNKNOWN_2010-SOUTH AFRICA | Johannesburg      |         64 |
| UNKNOWN_2002-KOREA/JAPAN  | Seoul             |         64 |
| UNKNOWN_1990-ITALY        | Milan             |         52 |
| UNKNOWN_1994-USA          | Chicago           |         52 |
| UNKNOWN_1986-MEXICO       | Mexico City       |         52 |
| UNKNOWN_1982-SPAIN        | Barcelona         |         52 |
| UNKNOWN_1974-FRG          | Frankfurt am Main |         38 |
| UNKNOWN_1978-ARGENTINA    | Buenos Aires      |         38 |
+---------------------------+-------------------+------------+
10 rows in set (0.01 sec)

**Interprétation :**

- Les éditions anciennes (avant 2014) ne fournissent pas les noms de stades dans les données sources.  
- L’ETL regroupe donc les matchs par édition sous des stades “UNKNOWN_YYYY-COUNTRY”.  
- Les valeurs 64, 52, 38 correspondent au nombre total de matchs par édition.  
- Le résultat est cohérent avec le format des Coupes du Monde et la structure des données disponibles.

---

# 🟩 KPI 8 — Score le plus fréquent de l’histoire  
*(intéressant pour un modèle de prédiction)*

```sql
SELECT 
    CONCAT(home_result, '-', away_result) AS score,
    COUNT(*) AS occurrences
FROM match_final
GROUP BY score
ORDER BY occurrences DESC
LIMIT 10;
```
+-------+-------------+
| score | occurrences |
+-------+-------------+
| 1-0   |         119 |
| 2-1   |         109 |
| 1-1   |          91 |
| 0-0   |          79 |
| 2-0   |          73 |
| 0-1   |          63 |
| 3-1   |          53 |
| 1-2   |          43 |
| 0-2   |          38 |
| 3-0   |          37 |
+-------+-------------+
10 rows in set (0.01 sec)

**Interprétation :**  
Les scores 1–0, 2–1, 0–0 sont généralement les plus fréquents.

---

# 🟩 KPI 9 — Matchs les plus prolifiques (top 10)  
*(utile pour storytelling dans le rapport)*

```sql
SELECT 
    home_team,
    away_team,
    edition,
    date,
    home_result,
    away_result,
    (home_result + away_result) AS total_goals
FROM match_final
ORDER BY total_goals DESC
LIMIT 10;
```
+-----------+-------------+------------------+---------------------+-------------+-------------+-------------+
| home_team | away_team   | edition          | date                | home_result | away_result | total_goals |
+-----------+-------------+------------------+---------------------+-------------+-------------+-------------+
| Austria   | Switzerland | 1954-SWITZERLAND | 1954-01-01 00:00:00 |           7 |           5 |          12 |
| Brazil    | Poland      | 1938-FRANCE      | 1938-01-01 00:00:00 |           6 |           5 |          11 |
| Hungary   | Germany     | 1954-SWITZERLAND | 1954-01-01 00:00:00 |           8 |           3 |          11 |
| Hungary   | El Salvador | 1982-SPAIN       | 1982-01-01 00:00:00 |          10 |           1 |          11 |
| France    | Paraguay    | 1958-SWEDEN      | 1958-01-01 00:00:00 |           7 |           3 |          10 |
| Argentina | Mexico      | 1930-URUGUAY     | 1930-01-01 00:00:00 |           6 |           3 |           9 |
| Hungary   | South Korea | 1954-SWITZERLAND | 1954-01-01 00:00:00 |           9 |           0 |           9 |
| Germany   | Turkey      | 1954-SWITZERLAND | 1954-01-01 00:00:00 |           7 |           2 |           9 |
| France    | Germany     | 1958-SWEDEN      | 1958-01-01 00:00:00 |           6 |           3 |           9 |
| Serbia    | Zaire       | 1974-FRG         | 1974-01-01 00:00:00 |           9 |           0 |           9 |
+-----------+-------------+------------------+---------------------+-------------+-------------+-------------+
10 rows in set (0.00 sec)

---

# 🟩 KPI 10 — Nombre de matchs par tour (round)  
*(montre la structure du tournoi)*

```sql
SELECT 
    round,
    COUNT(*) AS nb_matches
FROM match_final
GROUP BY round
ORDER BY nb_matches DESC;
```
+-----------------------+------------+
| round                 | nb_matches |
+-----------------------+------------+
| group_stage           |        603 |
| quarter_final         |         74 |
| round_of_16           |         64 |
| semi_final            |         58 |
| final                 |         21 |
| third_place           |         18 |
| Round of 16           |         16 |
| Quarter-finals        |          8 |
| Group H               |          6 |
| Group F               |          6 |
| Group C               |          6 |
| Group A               |          6 |
| Group E               |          6 |
| Group B               |          6 |
| Group G               |          6 |
| Group D               |          6 |
| Matchday 6            |          4 |
| Matchday 13           |          4 |
| Matchday 9            |          4 |
| Matchday 5            |          4 |
| Matchday 11           |          4 |
| Matchday 7            |          4 |
| Matchday 3            |          4 |
| Semi-finals           |          4 |
| Matchday 12           |          4 |
| Matchday 4            |          4 |
| Matchday 8            |          4 |
| Matchday 10           |          4 |
| Matchday 2            |          3 |
| Matchday 1            |          1 |
| Match for third place |          1 |
| Third place play-off  |          1 |
+-----------------------+------------+
32 rows in set (0.00 sec)

---