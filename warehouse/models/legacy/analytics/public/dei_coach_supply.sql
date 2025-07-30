WITH coaches AS (

  SELECT * FROM {{ref('dei_coaches')}}

),

iso_639_languages AS (

  SELECT * FROM {{ref('dbt_iso_639_languages')}}

),

staffing_qualifications AS (

  SELECT
    flattened.value::varchar AS staffing_qualification,
    coach_id
  FROM coaches,
  lateral flatten (input => staffing_qualifications) flattened

),

staffing_languages AS (

  SELECT
    flattened.value::varchar AS iso_639_alpha2,
    coach_id
  FROM coaches,
  lateral flatten (input => staffing_languages) flattened

),

attribute_language AS (

  SELECT
    'language' AS attribute_scope,
    sl.iso_639_alpha2 AS attribute_value,
    iso.language AS attribute_label,
    sl.coach_id
  FROM staffing_languages AS sl
  LEFT OUTER JOIN iso_639_languages AS iso
    ON sl.iso_639_alpha2 = iso.alpha2

),

attribute_member_level AS (

  SELECT
    'member_level' AS attribute_scope,
    flattened.value::varchar AS attribute_value,
    NULL::text AS attribute_label,
    coach_id
  FROM coaches,
  lateral flatten (input => staffing_member_levels) flattened

),

attribute_industry AS (

  SELECT
    'industry' AS attribute_scope,
    flattened.value::varchar AS attribute_value,
    NULL::text AS attribute_label,
    coach_id
  FROM coaches,
  lateral flatten (input => staffing_industries) flattened

),

attribute_geo AS (

  SELECT
    'geo' AS attribute_scope,
    geo AS attribute_value,
    NULL::text AS attribute_label,
    coach_id
  FROM coaches

),

attribute_country AS (

  SELECT
    'country' AS attribute_scope,
    country_code AS attribute_value,
    country_name AS attribute_label,
    coach_id
  FROM coaches
  WHERE country_code IS NOT NULL

),

attribute_tier AS (

  SELECT
    'tier' AS attribute_scope,
    staffing_tier AS attribute_value,
    NULL::text AS attribute_label,
    coach_id
  FROM coaches
  WHERE staffing_tier IS NOT NULL

),

attribute_certification AS (

  SELECT
    'certification' AS attribute_scope,
    regexp_replace(staffing_qualification, '^certification_', '') AS attribute_value,
    NULL::text AS attribute_label,
    coach_id
  FROM staffing_qualifications
  WHERE staffing_qualification LIKE 'certification_%'

),

attribute_product AS (

  SELECT
    'product' AS attribute_scope,
    staffing_qualification AS attribute_value,
    NULL::text AS attribute_label,
    coach_id
  FROM staffing_qualifications
  -- product qualification isn't prefixed
  WHERE staffing_qualification IN ('insights', 'pathways', 'smb')

)

{% set columns = 'attribute_scope, attribute_value,
       COALESCE(attribute_label, attribute_value) AS attribute_label, coach_id'
 %}



, final as (
    select
      {{ columns }}
    from attribute_language

    union all

    select
      {{ columns }}
    from attribute_member_level

    union all

    select
      {{ columns }}
    from attribute_industry

    union all

    select
      {{ columns }}
    from attribute_geo

    union all

    select
      {{ columns }}
    from attribute_country

    union all

    select
      {{ columns }}
    from attribute_tier

    union all

    select
      {{ columns }}
    from attribute_certification

    union all

    select
      {{ columns }}
    from attribute_product
)

select
    {{ dbt_utils.surrogate_key(['coach_id', 'attribute_scope', 'attribute_value']) }} as coach_scope_value_key,
    coach_id,
    attribute_scope,
    attribute_value,
    attribute_label
from final
