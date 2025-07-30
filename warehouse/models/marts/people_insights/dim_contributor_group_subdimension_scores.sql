{{
  config(
    tags=['eu']
  )
}}

with assessment_contributors as (

    select * from {{ ref('dim_assessment_contributors') }}

),

reference_population_scores as (

    select * from {{ ref('fact_reference_population_construct_scores') }}

),

contributor_group_subdimension_scores as (

    select
        assessment_contributors.parent_assessment_id,
        assessment_contributors.response_assessment_id as assessment_id,
        assessment_contributors.role as contributor_role,
        scores.construct_key,
        assessment_contributors.contributor_id,
        scores.scale_score,
        scores.scale_score_mean
    from assessment_contributors
    inner join reference_population_scores as scores
        on assessment_contributors.response_assessment_id = scores.assessment_id
    where assessment_contributors.response_submitted_at < assessment_contributors.report_generated_at
          and scores.reference_population_id = scores.organization_reference_population_id
          and scores.construct_type in ('subdimension','persisted_construct')
    group by assessment_contributors.parent_assessment_id,
             assessment_contributors.response_assessment_id,
             assessment_contributors.role,
             scores.construct_key,
             assessment_contributors.contributor_id,
             scores.scale_score,
             scores.scale_score_mean
)

select 
    {{ dbt_utils.surrogate_key(['parent_assessment_id', 'contributor_id', 'construct_key', 'assessment_id']) }} as primary_key,
    contributor_group_subdimension_scores.*
from contributor_group_subdimension_scores


