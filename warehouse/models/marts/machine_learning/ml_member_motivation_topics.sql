{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH motivation_topics AS (

  SELECT
    member_key,
    topic_theme_id,
    rel_dist AS relative_distance,
    date_key AS classified_date_key
  FROM {{ source('machine_learning', 'motivation_topics') }}

)

, final as (

    select
        {{ dbt_utils.surrogate_key(['member_key', 'topic_theme_id']) }} as member_topic_theme_key,
        member_key,
        topic_theme_id,
        row_number() over (partition by member_key order by relative_distance asc)
          as topic_theme_rank,
        relative_distance,
        lead(relative_distance) over (partition by member_key order by relative_distance asc)
          - relative_distance as relative_distance_to_next_topic_theme,
        classified_date_key
    from motivation_topics

)

select * from final
