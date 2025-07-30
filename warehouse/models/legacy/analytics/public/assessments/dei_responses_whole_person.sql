WITH assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}

),

member_assessments AS (

  SELECT * FROM {{ref('dei_member_assessments')}}

),

complete_wpm_assessments AS (

  SELECT * FROM {{ref('dbt_complete_wpm_assessments')}}

),

item_definition_whole_person AS (

  SELECT * FROM {{ref('item_definition_whole_person')}}

),

complete_wpm_assessment_items AS (

  SELECT
    *
  FROM assessment_items
  WHERE assessment_id IN (SELECT assessment_id FROM complete_wpm_assessments)

)

, final as (

    select
        {{ dbt_utils.surrogate_key(['cw.assessment_id', 'cw.item_key']) }} as assessment_item_key,
        cw.type as source,
        cw.assessment_id,
        cw.created_at,
        cw.item_key,
        cw.item_response::int as item_response,  -- Assuming WPM response is only an integer
        cw.sequence,  -- Sequence iterates within each source
        cw.submitted_at,
        cw.user_id as member_id,
        ma.track_id,
        wp.dimension,
        wp.factor,
        wp.item_prompt,
        wp.reverse_scored,
        wp.scale,
        wp.subdimension
    from complete_wpm_assessment_items as cw
    inner join item_definition_whole_person as wp
        on cw.item_key = wp.item_key
    inner join member_assessments as ma
        on cw.assessment_id = ma.assessment_id

)

select * from final
