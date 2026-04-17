-- dim_team.sql
-- All distinct team names across every World Cup edition.

with teams as (

    -- 1930-2010: strip native-language translations, e.g. "Brazil (Brasil)" → "Brazil"
    select trim(regexp_replace(team1, '\s*\(.*?\)', '')) as team_name
    from {{ ref('matches_19302010') }}
    where year < 2014
      and not regexp_matches(round, '^PRELIMINARY')

    union

    select trim(regexp_replace(team2, '\s*\(.*?\)', '')) as team_name
    from {{ ref('matches_19302010') }}
    where year < 2014
      and not regexp_matches(round, '^PRELIMINARY')

    union

    -- 2014
    select trim("Home Team Name") as team_name
    from {{ ref('world_cup_matches_2014') }}
    where "Home Team Goals" is not null

    union

    select trim("Away Team Name") as team_name
    from {{ ref('world_cup_matches_2014') }}
    where "Home Team Goals" is not null

    union

    -- 2018
    select trim(team1) as team_name from {{ ref('worldcup_2018') }}

    union

    select trim(team2) as team_name from {{ ref('worldcup_2018') }}

    union

    -- 2022
    select trim(team1) as team_name from {{ ref('worldcup_2022') }}

    union

    select trim(team2) as team_name from {{ ref('worldcup_2022') }}

)

select
    row_number() over (order by team_name) as id_team,
    team_name
from teams
where team_name is not null and team_name != ''
