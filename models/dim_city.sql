-- dim_city.sql
-- Distinct host cities across all World Cup editions (1930–2022).
-- 1930-2010: venue field used as city (no separate city column available).
-- 2014:      City column.
-- 2022:      city extracted from ground field ("Stadium, City").

with cities as (

    -- 1930-2010: venue = "Montevideo." → strip trailing dot
    select
        trim(trailing '.' from trim(venue)) as city_name
    from {{ ref('matches_19302010') }}
    where year < 2014
      and not regexp_matches(round, '^PRELIMINARY')
      and venue is not null

    union

    -- 2014
    select trim("City") as city_name
    from {{ ref('world_cup_matches_2014') }}
    where "Home Team Goals" is not null

    union

    -- 2018 & 2022: "Stadium, City" → "City"
    select
        trim(regexp_extract(ground, ',\s*([^,]+)$', 1)) as city_name
    from {{ ref('worldcup_2018') }}

    union

    select
        trim(regexp_extract(ground, ',\s*([^,]+)$', 1)) as city_name
    from {{ ref('worldcup_2022') }}

)

select
    row_number() over (order by city_name) as id_city,
    city_name
from cities
where city_name is not null and city_name != ''
