{{
  config({
    "materialized" : "view",
    "tags": ['classification.c2_restricted']
  })
}}


WITH assessments AS (

  SELECT * FROM {{ref('int_app__assessments')}}

)


, final as (

    select
        {{ dbt_utils.surrogate_key(['a.assessment_id', 'r.path']) }} as assessment_item_key,
        row_number() over (partition by a.user_id, a.type order by a.submitted_at) as sequence,
        a.assessment_id,
        a.user_id,
        a.creator_id,
        a.type,
        a.questions_version,
        a.created_at,
        a.submitted_at,
        r.path as item_key,
        r.value::string as item_response
    from assessments as a
    join lateral flatten (input => a.responses) as r
    where submitted_at is not null
        -- discard empty or uninformative responses
        and (r.value is not null and r.value != '')

)

select * from final
