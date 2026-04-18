-- ============================================================
--  QUALITY CHECKS FOR FOOT DATAWAREHOUSE
--  Vérifications d'intégrité, de cohérence et de qualité
-- ============================================================

USE foot;

-- ============================================================
-- 1. Vérification des dimensions non vides
-- ============================================================

SELECT 'team'   AS table_name, COUNT(*) AS row_count FROM team;
SELECT 'city'   AS table_name, COUNT(*) AS row_count FROM city;
SELECT 'stadium' AS table_name, COUNT(*) AS row_count FROM stadium;
SELECT 'edition' AS table_name, COUNT(*) AS row_count FROM edition;

-- ============================================================
-- 2. Vérification des clés étrangères dans match
-- ============================================================

SELECT COUNT(*) AS missing_home_team
FROM `match`
WHERE id_home_team IS NULL;

SELECT COUNT(*) AS missing_away_team
FROM `match`
WHERE id_away_team IS NULL;

SELECT COUNT(*) AS missing_stadium
FROM `match`
WHERE id_stadium IS NULL;

SELECT COUNT(*) AS missing_edition
FROM `match`
WHERE id_edition IS NULL;

-- ============================================================
-- 3. Vérification de la cohérence match → stadium → city
-- ============================================================

SELECT COUNT(*) AS missing_city
FROM `match` m
LEFT JOIN stadium s ON m.id_stadium = s.id_stadium
LEFT JOIN city c ON s.id_city = c.id_city
WHERE c.id_city IS NULL;

-- ============================================================
-- 4. Vérification des doublons dans les dimensions
-- ============================================================

SELECT city_name, COUNT(*) AS n
FROM city
GROUP BY city_name
HAVING n > 1;

SELECT stadium_name, COUNT(*) AS n
FROM stadium
GROUP BY stadium_name
HAVING n > 1;

SELECT team_name, COUNT(*) AS n
FROM team
GROUP BY team_name
HAVING n > 1;

-- ============================================================
-- 5. Vérification des dates invalides
-- ============================================================

SELECT COUNT(*) AS invalid_dates
FROM `match`
WHERE date IS NULL;

SELECT COUNT(*) AS dates_before_1900
FROM `match`
WHERE date < '1900-01-01';

-- ============================================================
-- 6. Vérification des scores incohérents
-- ============================================================

SELECT COUNT(*) AS negative_scores
FROM `match`
WHERE home_result < 0 OR away_result < 0;

-- ============================================================
-- 7. Vérification des équipes non utilisées
-- ============================================================

SELECT 
    t.id_team,
    t.team_name
FROM team t
LEFT JOIN `match` m 
    ON t.id_team = m.id_home_team
    OR t.id_team = m.id_away_team
WHERE m.id_match IS NULL;

-- ============================================================
-- 8. Vérification des villes non utilisées par un stade
--    (analyse, pas une erreur)
-- ============================================================

SELECT 
    c.id_city,
    c.city_name
FROM city c
LEFT JOIN stadium s ON c.id_city = s.id_city
WHERE s.id_city IS NULL;

-- ============================================================
-- 9. Vérification des stades non utilisés dans un match
-- ============================================================

SELECT 
    s.id_stadium,
    s.stadium_name
FROM stadium s
LEFT JOIN `match` m ON s.id_stadium = m.id_stadium
WHERE m.id_match IS NULL;

-- ============================================================
-- 10. Vérification de la cohérence des éditions
-- ============================================================

SELECT 
    e.id_edition,
    e.edition_name
FROM edition e
LEFT JOIN `match` m ON e.id_edition = m.id_edition
WHERE m.id_match IS NULL;

-- ============================================================
-- FIN DU SCRIPT DE QUALITY CHECKS
-- ============================================================
