
with t as (
    select 
        a.assessment_id as assessment_id,
        a.submitted_at as submitted_at,
        a.assessment_type as assessment_type,
        air.item_key as item_key,
        air.assessment_item_id as item_id
    from {{ ref('fact_assessments') }} a 
    join {{ ref('fact_assessment_item_responses') }} air on a.assessment_id = air.assessment_id
)

select
    {{ dbt_utils.surrogate_key(['assessment_id', 'item_key', 'item_id']) }} as primary_key,
    assessment_id,
    submitted_at,
    assessment_type,
    item_key 
from t