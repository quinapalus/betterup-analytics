{{
  config(
    tags=["eu"]
  )
}}

--setting variables
{%- set current_year = run_started_at.strftime("%Y") | int  -%}
{%- set seasons = ['Spring ','Autumn '] -%}
{%- set spring_start_month = 3 | int -%}
{%- set autumn_start_month = 9 | int -%}
{%- set start_day = 1 | int %}

WITH seasons AS (

  --creating pre_season 
  SELECT 
    'Pre-Season' AS season_name,
    '1/1/2013'::date AS season_start_date,
    '3/1/2021'::date AS season_end_date

  UNION ALL

  --creating live_season
  SELECT 
    'Live' AS season_name ,
    date_trunc('month',current_date) - interval '365 DAYS' AS season_start_date,
    date_trunc('month',current_date) AS season_end_date
   
   UNION ALL
   --creating spring and fall seasons

{%- for season in seasons -%}
{%- for year in range(2022,current_year+3) %}

  SELECT 
    '{{ season }}' || cast({{ year }} AS varchar) AS season_name,
  {%- if   season   == 'Spring ' %} 
    date_from_parts({{ year }}-1, {{ spring_start_month }}, {{ start_day }}) AS season_start_date,
    date_from_parts({{ year }}, {{ spring_start_month }}, {{ start_day }}) AS season_end_date
  {%- else %}
    date_from_parts({{ year }}-1, {{ autumn_start_month }}, {{ start_day }}) AS season_start_date,
    date_from_parts({{ year }}, {{ autumn_start_month }}, {{ start_day }}) AS season_end_date
  {%- endif %}
  
{% if not loop.last %} UNION ALL {% endif %} 
{%- endfor -%}
{% if not loop.last %} UNION ALL {% endif %} 
{%- endfor -%}

)
--adding surrogate key

    
  SELECT 
    {{ dbt_utils.surrogate_key(['season_name']) }} AS season_id,
    seasons.* 
  FROM seasons
