-- dim_edition.sql
-- One row per World Cup tournament with year and host country name.

with editions as (

    -- 1930-2010: edition field = "1930-URUGUAY"
    select distinct
        year::integer                                        as year,
        replace(
            regexp_extract(edition, '-(.+)$', 1), '_', ' '
        )                                                    as edition_name
    from {{ ref('matches_19302010') }}
    where year < 2014
      and not regexp_matches(round, '^PRELIMINARY')
      and edition is not null

    union all select 2014, 'Brazil'
    union all select 2018, 'Russia'
    union all select 2022, 'Qatar'

)

select
    row_number() over (order by year) as id_edition,
    edition_name,
    year
from editions
