{{ config(
    tags=["identify_ai_metrics"],
) }}

{%- set indicators = {
  'member_engagement': 'avg_session_hours_last60days',
  'member_event': 'variable_work_life_events',
  'member_satisfaction': 'avg_use_of_time_rating',
  'member_growth': 'percent_growth_from_reference',
}
-%}

WITH leading_indicators AS (

  SELECT * FROM {{ref('int_iai_leading_indicators')}}

),

lagging_indicators AS (

    SELECT * FROM {{ref('int_iai_lagging_indicators')}}

),

recommendation_indicators AS (

    SELECT
        indicator_name, weight
    FROM {{ref('stg_app__recommendation_indicators')}}
    QUALIFY ROW_NUMBER()OVER(PARTITION BY indicator_name ORDER BY created_at DESC) = 1
),

indicators AS (

    SELECT
        lead.member_id,
        lead.reporting_group_id,
        {%- for alias, indicator in indicators.items() -%} {{indicator}} AS {{alias}} {% if not loop.last %} , {% endif %} {%- endfor -%}
    FROM leading_indicators AS lead
    JOIN lagging_indicators AS lag
        ON lead.member_id = lag.member_id AND
        lead.reporting_group_id = lag.reporting_group_id

),

max_indicators_per_rg AS (

    SELECT
        reporting_group_id,
        {%- for indicator in indicators -%}
            MAX({{indicator}}) AS max_{{indicator}} {% if not loop.last %} , {% endif %}
        {%- endfor -%}
    FROM indicators
    GROUP BY reporting_group_id

),

score_per_indicator AS (

  {%- for indicator in indicators -%}

    (SELECT
        member_id,
        ind.reporting_group_id,
        '{{indicator}}' AS indicator,
        CASE
            WHEN
              {{indicator}} IS NULL
            THEN NULL
            WHEN
              {{indicator}} <= 0 OR
              max_{{indicator}} <= 0
            THEN 0
            ELSE ({{indicator}}/max_{{indicator}})*(SELECT weight FROM recommendation_indicators WHERE indicator_name = '{{indicator}}')
        END AS score
    FROM indicators AS ind
    JOIN max_indicators_per_rg AS mi
        ON ind.reporting_group_id = mi.reporting_group_id
    )

    {% if not loop.last %} UNION ALL {% endif %}

  {%- endfor -%}

)

-- getting rid of those members who don't score on at least one indicator

, score_per_indicator_clean as (
SELECT member_id,
    reporting_group_id,
    score,
    indicator
FROM score_per_indicator
    QUALIFY BOOLOR_AGG(IFF(score IS NULL, FALSE, TRUE)) OVER (PARTITION BY member_id, reporting_group_id) = TRUE
)

-- getting weighted average of scores for only those indicators which have values

  , score_per_rg_per_member as(
SELECT
    {{ dbt_utils.surrogate_key(['member_id', 'reporting_group_id']) }} AS primary_key,
    reporting_group_id,
    member_id,
    SUM(COALESCE(score,0))/SUM(IFF(score IS NULL, 0, ri.weight)) AS score
FROM score_per_indicator_clean AS spi
JOIN recommendation_indicators AS ri
    ON ri.indicator_name = spi.indicator
GROUP BY reporting_group_id, member_id
)

-- min_max_scores per rg
, min_max_scores AS(
  SELECT
      reporting_group_id,
      min(score) AS min_score,
      max(score) AS max_score
  FROM score_per_rg_per_member
  GROUP BY reporting_group_id
)

/*
 We have a number of reporting groups that only have 1 member and thus the min and max scores will be the same.
 To avoid division by zero error, we're grouping all these members into a temporary group
 */
, normalizer as (
    SELECT
        IFF(mms.min_score = mms.max_score, 0000, sprpm.reporting_group_id) as reporting_group_id,
        MEMBER_ID,
        score
    FROM score_per_rg_per_member AS sprpm
    JOIN min_max_scores as mms
        ON sprpm.reporting_group_id = mms.reporting_group_id
)

/*
 getting the min and max per reporting group
 */
, normalizer_min_max_scores AS (
  SELECT
      reporting_group_id,
      min(score) AS min_score,
      max(score) AS max_score
  FROM normalizer
  GROUP BY reporting_group_id
)

-- normalised score per rg per member
SELECT
    sprpm.primary_key,
    sprpm.reporting_group_id,
    sprpm.member_id,
-- normalised score = [(score - min_score)/(max_score - min_score)] * max_value
    ((n.score - nmms.min_score)/(nmms.max_score - nmms.min_score)) * 1 AS score
FROM normalizer n
JOIN normalizer_min_max_scores nmms ON n.reporting_group_id = nmms.reporting_group_id
JOIN score_per_rg_per_member as sprpm
    ON sprpm.member_id = n.member_id
    AND n.reporting_group_id = sprpm.reporting_group_id
