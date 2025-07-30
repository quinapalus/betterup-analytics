
with t as (
    select 
        a.assessment_id as assessment_id,
        a.submitted_at as submitted_at,
        a.assessment_type as assessment_type,
        wpm.subdimension_key as subdimension_key
    from {{ ref('fact_assessments') }} a 
    join {{ ref('dbt__whole_person_subdimension_scores') }} wpm on a.assessment_id = wpm.assessment_id
)

select
    {{ dbt_utils.surrogate_key(['assessment_id', 'subdimension_key']) }} as primary_key,
    assessment_id,
    submitted_at,
    assessment_type,
    subdimension_key 
from t