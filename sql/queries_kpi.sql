-- ============================================
-- KPI 1 : Nombre de matchs par édition
-- ============================================
SELECT 
    edition,
    COUNT(*) AS nb_matches
FROM match_final
GROUP BY edition
ORDER BY edition;

-- ============================================
-- KPI 2 : Nombre total de buts par édition
-- ============================================
SELECT 
    edition,
    SUM(home_result + away_result) AS total_goals
FROM match_final
GROUP BY edition
ORDER BY edition;

-- ============================================
-- KPI 3 : Moyenne de buts par match par édition
-- ============================================
SELECT 
    edition,
    ROUND(AVG(home_result + away_result), 2) AS avg_goals_per_match
FROM match_final
GROUP BY edition
ORDER BY edition;

-- ============================================
-- KPI 4 : Répartition des résultats
-- ============================================
SELECT 
    result,
    COUNT(*) AS nb_matches,
    ROUND(100 * COUNT(*) / (SELECT COUNT(*) FROM match_final), 2) AS pct
FROM match_final
GROUP BY result
ORDER BY nb_matches DESC;

-- ============================================
-- KPI 5 : Équipe la plus victorieuse
-- ============================================
SELECT 
    result AS winning_team,
    COUNT(*) AS wins
FROM match_final
WHERE result <> 'draw'
GROUP BY winning_team
ORDER BY wins DESC
LIMIT 10;

-- ============================================
-- KPI 6 : Matchs par ville
-- ============================================
SELECT 
    city,
    COUNT(*) AS nb_matches
FROM match_final
GROUP BY city
ORDER BY nb_matches DESC;

-- ============================================
-- KPI 7 : Matchs par stade
-- ============================================
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

-- ============================================
-- KPI 8 : Score le plus fréquent
-- ============================================
SELECT 
    CONCAT(home_result, '-', away_result) AS score,
    COUNT(*) AS occurrences
FROM match_final
GROUP BY score
ORDER BY occurrences DESC
LIMIT 10;

-- ============================================
-- KPI 9 : Matchs les plus prolifiques
-- ============================================
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

-- ============================================
-- KPI 10 : Matchs par tour
-- ============================================
SELECT 
    round,
    COUNT(*) AS nb_matches
FROM match_final
GROUP BY round
ORDER BY nb_matches DESC;
