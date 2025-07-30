{{
  config(
    tags=['classification.c3_confidential','eu'],
    materialized='table'
  )
}}

{%- set features = [
  'dbt_features__extended_network_session_types',
  'dbt_features__group_coaching_assessment_item_responses',
  'dbt_features__group_coaching_minutes',
  'dbt_features__group_coaching_topics',
  'dbt_features__post_session_assessment_item_responses',
  'dbt_features__member_testimonials',
  'dbt_features__session_topics',
  'dbt_features__care_mental_fitness_scores'
  ]
  -%}

{%- for feature in features -%}

SELECT
  {{ dbt_utils.surrogate_key(['member_id', 'associated_record_type', 'associated_record_id', 'feature_key', 'feature_collected_at']) }} AS primary_key,
  member_id,
  associated_record_id,
  associated_record_type,
  feature_collected_at,
  feature_key,
  classification,
  feature_attributes,
  feature_type
FROM {{ ref(feature) }}
{% if not loop.last %} UNION ALL {% endif %}

{%- endfor -%}