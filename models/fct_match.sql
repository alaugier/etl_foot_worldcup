-- fct_match.sql
-- One row per World Cup match (finals only, no qualifiers) — all editions 1930-2022.
-- FKs to dim_team (home & away), dim_stadium, dim_edition.

with

-- 1930-2010
hist as (
    select
        trim(regexp_replace(team1, '\s*\(.*?\)', ''))   as home_team,
        trim(regexp_replace(team2, '\s*\(.*?\)', ''))   as away_team,
        cast(regexp_extract(score, '(\d+)-\d+', 1) as integer)  as home_result,
        cast(regexp_extract(score, '\d+-(\d+)',   1) as integer) as away_result,
        cast(year::text || '-01-01' as date)             as match_date,

        case round
            when 'GROUP_STAGE'        then 'Group stage'
            when 'FIRST'              then 'Group stage'
            when 'FINAL_ROUND'        then 'Group stage'
            when 'SEMIFINAL_STAGE'    then 'Semi-final'
            when 'QUARTERFINAL_STAGE' then 'Quarter-final'
            when '1/8_FINAL'          then 'Round of 16'
            when '1/4_FINAL'          then 'Quarter-final'
            when '1/2_FINAL'          then 'Semi-final'
            when '_FINAL'             then 'Final'
            when 'PLACES_3&4'         then 'Third place'
            else round
        end                                              as round,

        trim(trailing '.' from trim(venue))             as stadium_name,
        year::integer                                    as edition_year

    from {{ ref('matches_19302010') }}
    where year < 2014
      and not regexp_matches(round, '^PRELIMINARY')
      and score != 'xxx'
    qualify row_number() over (
        partition by trim(regexp_replace(team1,'\s*\(.*?\)',''))
                   , trim(regexp_replace(team2,'\s*\(.*?\)',''))
                   , year
                   , score
        order by 1
    ) = 1
),

-- 2014
wc2014 as (
    select
        trim("Home Team Name")                           as home_team,
        trim("Away Team Name")                           as away_team,
        "Home Team Goals"                                as home_result,
        "Away Team Goals"                                as away_result,
        strptime(trim("Datetime"), '%d %b %Y - %H:%M')::date as match_date,
        trim("Stage")                                    as round,
        trim("Stadium")                                  as stadium_name,
        2014                                             as edition_year

    from {{ ref('world_cup_matches_2014') }}
    where "Home Team Goals" is not null
    qualify row_number() over (
        partition by "MatchID" order by 1
    ) = 1
),

-- 2018
wc2018 as (
    select
        trim(team1)                                      as home_team,
        trim(team2)                                      as away_team,
        home_result::integer                             as home_result,
        away_result::integer                             as away_result,
        date::date                                       as match_date,
        round,
        trim(regexp_extract(ground, '^([^,]+)', 1))      as stadium_name,
        2018                                             as edition_year

    from {{ ref('worldcup_2018') }}
),

-- 2022
wc2022 as (
    select
        trim(team1)                                      as home_team,
        trim(team2)                                      as away_team,
        home_result::integer                             as home_result,
        away_result::integer                             as away_result,
        date::date                                       as match_date,

        coalesce(
            case when "group" is not null then "group" end,
            round
        )                                                as round,

        trim(regexp_extract(ground, '^([^,]+)', 1))      as stadium_name,
        2022                                             as edition_year

    from {{ ref('worldcup_2022') }}
),

-- union all sources
all_matches as (
    select * from hist
    union all
    select * from wc2014
    union all
    select * from wc2018
    union all
    select * from wc2022
),

-- join dims
with_keys as (
    select
        m.*,
        case
            when m.home_result > m.away_result then 'home'
            when m.away_result > m.home_result then 'away'
            else 'draw'
        end                  as result,
        ht.id_team           as id_home_team,
        away_t.id_team       as id_away_team,
        s.id_stadium,
        e.id_edition
    from all_matches m
    left join {{ ref('dim_team') }}    ht      on ht.team_name     = m.home_team
    left join {{ ref('dim_team') }}    away_t  on away_t.team_name = m.away_team
    left join {{ ref('dim_stadium') }} s       on s.stadium_name   = m.stadium_name
    left join {{ ref('dim_edition') }} e       on e.year           = m.edition_year
)

select
    row_number() over (order by edition_year, match_date) as id_match,
    match_date                                            as date,
    round,
    home_result,
    away_result,
    result,
    id_home_team,
    id_away_team,
    id_stadium,
    id_edition
from with_keys
