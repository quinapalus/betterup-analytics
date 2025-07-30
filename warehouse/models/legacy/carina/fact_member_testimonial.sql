{{
  config(
    tags=['classification.c3_confidential']
  )
}}


WITH testimonials AS (

  SELECT * FROM {{ref('stg_app__testimonials')}}

),

member_assessments AS (

  SELECT * FROM {{ref('dei_member_assessments')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}} 
  where is_external and engaged_member_count is not null --this logic was in dei_tracks which this model used to reference

),

dim_member AS (

  SELECT * FROM {{ref('dim_members')}}

),

dim_account AS (

  SELECT * FROM {{ref('dim_account')}}

),

final as (
SELECT distinct
  {{ member_key('ma.member_id') }} AS member_key,
  {{ date_key('ma.submitted_at') }} AS date_key,
  {{ account_key('tr.organization_id', 'tr.sfdc_account_id') }} AS account_key,
  {{ deployment_key('ma.track_id') }} AS deployment_key,
  te.text
FROM testimonials AS te
INNER JOIN member_assessments AS ma
  ON te.assessment_id = ma.assessment_id
INNER JOIN tracks AS tr
  ON ma.track_id = tr.track_id
WHERE
  -- ensure foreign keys are present in dimension tables
  {{member_key('ma.member_id')}} IN (SELECT member_key FROM dim_member) AND
  {{account_key('tr.organization_id', 'tr.sfdc_account_id')}} IN (SELECT account_key FROM dim_account)
)

select
    {{ dbt_utils.surrogate_key(['member_key', 'date_key', 'account_key', 'deployment_key', 'text' ]) }} as primary_key,
    *
from final