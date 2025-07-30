{{
  config(
    tags=['classification.c3_confidential','eu']
  )
}}

WITH members AS (

  SELECT * FROM {{ref('dei_members')}}

),

member_hcm_attributes AS (

  SELECT * FROM {{ref('dei_member_attributes_sanitized')}}
  -- Filter to only include fields that have been whitelisted in config file:
  WHERE category_name IN (
    SELECT internal_field_name
    FROM {{ref('carina_hcm_attribute_fields')}}
  )

),

-- load whitelisted fields into jinja variable that we can iterate through in final query
{% set fields = dbt_utils.get_column_values(table=ref('carina_hcm_attribute_fields'), column='internal_field_name') %}


joined as (
SELECT
  {{ member_key('m.member_id') }} AS member_key,
  m.member_id AS app_member_id,
  m.geo AS member_geo,
  m.subregion_m49 AS member_subregion_m49,
  m.level AS employee_level,
  -- iterate over whitelisted fields to convert each field into a column
  -- Note: field names are stored as Title Case in carina_hcm_attribute_fields.csv
  -- so we convert to snake_case before aliasing the column name
  {% for field in fields %}
    {% set snaked_field = field.lower().replace(' ', '_') %}
    ma_{{ snaked_field }}.value AS member_hcm_attribute_{{ snaked_field }}
    {%- if not loop.last -%},{% endif %}
  {% endfor %}
FROM members AS m
-- iterate over whitelisted fields, joining and aliasing a filtered version
-- of the row-based member_hcm_attributes table so we can convert to columns above
{% for field in fields %}
  {% set snaked_field = field.lower().replace(' ', '_') %}
  LEFT OUTER JOIN member_hcm_attributes AS ma_{{ snaked_field }}
    ON ma_{{ snaked_field }}.category_name = '{{ field }}' AND
       m.member_id = ma_{{ snaked_field }}.member_id
{% endfor %}
),

final as (
  select
  {{dbt_utils.surrogate_key(['member_key', 'app_member_id', 
                            'member_geo', 'member_subregion_m49',
                            'employee_level', 'member_hcm_attribute_work_location',
                            'member_hcm_attribute_country', 'member_hcm_attribute_region'])}} as _unique,
    *
  from joined
),

dedup as (
  select
    *
  from final
  --removes 30 records that should not be in this dataset.
  qualify(row_number() over (partition by _unique order by member_key) = 1)
)

select * from dedup