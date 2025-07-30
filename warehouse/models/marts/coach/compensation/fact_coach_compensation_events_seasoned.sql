--this fact table takes the comp events that we need for seasonal coach reporting and joins it to the necessary seasons


{# we only want this model to refresh 1x per month on the 1st day of the month. #} 
{# see this story for context https://betterup.atlassian.net/browse/DATA-1506?atlOrigin=eyJpIjoiYzMzNGQ3OTRmOTMzNDRmN2E4ZDBkY2EyZDkyNTZjNGIiLCJwIjoiaiJ9 #}
{# When running this model in dev (target name = 'dev') the jinja will always a full refresh on the model. #}
{#In prod (target name = 'prod') the full model will only run on 1st day of each month #}

{%- set current_day_of_month = run_started_at.strftime('%-d') | int %}
{%- if current_day_of_month != 1  and target.name == 'prod'-%}

  select * from {{ this }} 

  {{- log("Skipping refresh on " ~ model["unique_id"] ~ " for " ~ target.name, info = true) -}}

{%- else -%}

{%- set events = [
  'dbt_events__completed_coaching_session',
  'dbt_events__submitted_post_session_assessment',
  'dbt_events__completed_billable_event'
  ]
  -%}

WITH dim_seasons AS (
  SELECT * FROM {{ ref('dim_coach_compensation_seasons') }}
)

{%- for event in events -%}

SELECT
  {{ dbt_utils.surrogate_key(['member_id','coach_id','associated_record_id', 'associated_record_type', 'event_action', 'event_object', 'event_at','season_id']) }} AS primary_key,
  dim_seasons.season_id,
  {{ ref(event) }}.*
FROM {{ ref(event) }}
LEFT JOIN dim_seasons 

{%- if event == 'dbt_events__completed_coaching_session' %}
  on DATEADD('day', 365, {{ ref(event) }}.attributes:first_completed_primary_b2b_coach_assignment_session_date) >= season_start_date
    --this join condition is needed so that we can do the five in first ninety day metric. By pushing it up to 365 it gives us the flexibility 
    --to do various x in y days metrics without having to update this fact model

{%- elif event == 'dbt_events__completed_billable_event' %}
  on {{ ref(event) }}.event_at <= dim_seasons.season_end_date
{%- else %}
  on {{ ref(event) }}.event_at >= dim_seasons.season_start_date
     and {{ ref(event) }}.event_at < dim_seasons.season_end_date
{%- endif %}
{% if not loop.last %} UNION ALL {% endif %}
{%- endfor %}
{%- endif %}

