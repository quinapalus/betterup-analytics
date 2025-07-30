{{
  config(
    tags=['eu'],
    materialized='table'
  )
}}

with motivation_themes as (
  select * from {{ ref('member_motivation_themes') }}
),

topics_filtered as (
  select
    member_id,
    motivation,
    topic1_score,
    topic2_score,
    topic3_score,
    topic_theme1_id as theme1,
    case
      when abs(topic1_score - topic2_score) < 0.2
      and topic2_score > 0.2
      then topic_theme2_id else null
    end as theme2,
    case
      when abs(topic1_score - topic3_score) < 0.2
      and topic3_score > 0.2
      then topic_theme3_id else null
    end as theme3,
    assessment_id,
    scored_at,
    submitted_at
  from motivation_themes -- analytics.ml.motivation_themes
),

topics_filtered_unpivoted as (
  select *
  from topics_filtered
  unpivot(
    topic_theme_id
    FOR topic_theme_rank in (theme1, theme2 , theme3)
  )
),

member_motivation_themes AS (
  select
    topic_theme_id,
    trim(topic_theme_rank, 'THEME')::int as topic_theme_rank,
    case
      when topic_theme_rank = 'THEME1' then topic1_score
      when topic_theme_rank = 'THEME2' then topic2_score
      when topic_theme_rank = 'THEME3' then topic3_score
    end as topic_score,
    case
      when topic_theme_rank = 'THEME1' then topic1_score-topic2_score
      when topic_theme_rank = 'THEME2' then topic2_score-topic3_score
      when topic_theme_rank = 'THEME3' then NULL
    end as relative_distance_to_next_topic_theme,
    motivation,
    member_id,
    assessment_id,
    submitted_at,
    scored_at,
    topic_theme_id is not null as is_proximal_topic_theme,
    topic_theme_rank || ': ' || t.name || IFF(trim(topic_theme_rank, 'THEME')::int > 1, ' (' || to_char(topic_score, 'FM0d000') || ')', '')
      as ranked_topic_theme_label,
    mt.scored_at as scored_date,
    left(scored_date, charindex(' ', scored_date) - 1) as classified_date
  from topics_filtered_unpivoted as mt
  inner join {{ ref('topic_themes') }} as t
    on mt.topic_theme_id = t.id
   -- limit to most recent motivation classified per member:
   qualify row_number() over (partition by mt.member_id, mt.topic_theme_id order by mt.scored_at desc) = 1
),

member_motivation_metrics as (
  -- calculate motivation-level metrics:
  select
    member_id,
    sum(case when is_proximal_topic_theme then 1 end) as proximal_themes_count,
    listagg(ranked_topic_theme_label, ',\n') within group (order by topic_theme_rank) as ranked_theme_list
  from member_motivation_themes
  group by member_id
),

member_motivation_final as (
  select *,
  case when proximal_themes_count >= 4 then '4 or more' else to_char(proximal_themes_count) end as proximal_themes_category,
  proximal_themes_count < 4 as is_motivation_reasonably_focused
  from member_motivation_metrics
),

final as (
        -- join motivation-level metrics back into original motivation-theme dataset:
  select

  --surrogate key
  {{ dbt_utils.surrogate_key(['t.member_id', 't.topic_theme_id'])}} as member_motivation_topic_id,

  --foreign keys
  t.assessment_id,
  t.member_id,
  t.topic_theme_id,

  --logical data
  t.motivation,
  m.proximal_themes_category,
  m.proximal_themes_count,
  t.relative_distance_to_next_topic_theme,
  t.ranked_topic_theme_label,
  m.ranked_theme_list,
  t.topic_theme_rank,
  t.topic_score,

  --booleans
  m.is_motivation_reasonably_focused, 
  m.is_motivation_reasonably_focused and t.is_proximal_topic_theme as is_relevant_topic_theme,
  t.topic_theme_rank = 1 as is_primary_topic_theme,
  t.is_proximal_topic_theme,

  --timestamps
  t.classified_date,
  t.scored_at,
  t.scored_date,
  t.submitted_at
  
  from member_motivation_themes as t
  inner join member_motivation_final as m
    on t.member_id = m.member_id
)

select *
from final
