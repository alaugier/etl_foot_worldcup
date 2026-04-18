-- Vérification 1 : doublons dans la table match
SELECT 
    id_match, 
    COUNT(*) AS nb
FROM `match`
GROUP BY id_match
HAVING nb > 1;

-- Vérification 2 : matchs sans stade associé
SELECT 
    id_match, 
    id_stadium
FROM `match`
WHERE id_stadium IS NULL;

-- Vérification 3 : stades sans ville associée
SELECT 
    id_stadium, 
    stadium_name
FROM stadium
WHERE id_city IS NULL;

-- Vérification 4 : villes non utilisées
SELECT 
    c.id_city, 
    c.city_name
FROM city c
LEFT JOIN stadium s ON c.id_city = s.id_city
WHERE s.id_city IS NULL;

-- Vérification 5 : équipes non utilisées
SELECT 
    t.id_team, 
    t.team_name
FROM team t
LEFT JOIN `match` m 
    ON t.id_team = m.id_team_home 
    OR t.id_team = m.id_team_away
WHERE m.id_match IS NULL;

-- Vérification 6 : incohérences de score
SELECT 
    id_match, 
    score_home, 
    score_away
FROM `match`
WHERE score_home < 0 OR score_away < 0;

-- Vérification 7 : matchs sans date
SELECT 
    id_match
FROM `match`
WHERE match_date IS NULL;

-- Vérification 8 : dates invalides
SELECT 
    id_match, 
    match_date
FROM `match`
WHERE match_date < '1900-01-01';
