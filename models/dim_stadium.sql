-- dim_stadium.sql
-- Distinct stadiums with their city FK.

with stadiums as (

    -- 1930-2010: venue used as both stadium and city
    select
        trim(trailing '.' from trim(venue)) as stadium_name,
        trim(trailing '.' from trim(venue)) as city_name
    from {{ ref('matches_19302010') }}
    where year < 2014
      and not regexp_matches(round, '^PRELIMINARY')
      and venue is not null

    union

    -- 2014
    select
        trim("Stadium") as stadium_name,
        trim("City")    as city_name
    from {{ ref('world_cup_matches_2014') }}
    where "Home Team Goals" is not null

    union

    -- 2022: "Al Bayt Stadium, Al Khor" → stadium / city
    select
        trim(regexp_extract(ground, '^([^,]+)',    1)) as stadium_name,
        trim(regexp_extract(ground, ',\s*([^,]+)$', 1)) as city_name
    from {{ ref('worldcup_2022') }}

),

deduped as (
    select distinct stadium_name, city_name
    from stadiums
    where stadium_name is not null and stadium_name != ''
)

select
    row_number() over (order by stadium_name) as id_stadium,
    stadium_name,
    c.id_city
from deduped s
left join {{ ref('dim_city') }} c using (city_name)
