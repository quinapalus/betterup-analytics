{{
  config(
    tags=['eu']
  )
}}

WITH users AS (

  SELECT * FROM {{ref('dbt_users')}}

),

user_info AS (

  SELECT * FROM {{ref('stg_app__users')}}

),

coach_profiles AS (

  SELECT * FROM {{ref('int_coach__coach_profiles')}}

),

coach_specialist_vertical AS (

  SELECT * FROM {{ref('dbt_coach_specialist_vertical')}}

),

coach_assignments AS (

  SELECT * FROM {{ref('stg_app__coach_assignments')}}

),

track_assignments AS (

  SELECT * FROM {{ref('stg_app__track_assignments')}}

),

tracks AS (

  SELECT * FROM {{ref('dim_tracks')}}

),

wpm2_mock_sessions AS (

  SELECT * FROM {{ref('dbt_wpm2_mock_sessions')}}

),

priority_languages AS (

  SELECT * FROM {{ref('bu_priority_staffing_languages')}}

),

staffable_dates as (

    select * from {{ ref('int_coach__versions_rollup') }}

),

languages_by_coach AS (

  SELECT * FROM (

  SELECT
    p.coach_id,

    {{string_to_array()}}(lower(array_to_string(p.staffing_languages, ';')), ';')
        AS staffing_languages,

    pl.language AS ranked_language,
    pl.priority_rank,
    ROW_NUMBER() OVER (
        PARTITION BY p.coach_id
        ORDER BY p.coach_id, pl.priority_rank ASC NULLS LAST
    ) as index
  FROM coach_profiles AS p
  -- join if any of a coach's languages are in the priority staffing list
  LEFT OUTER JOIN priority_languages AS pl
    ON array_contains(pl.language::variant, p.staffing_languages) = true
  -- for each coach, select the language with the highest priority_rank

  ) a

WHERE index = 1

),

open_members_by_coach AS (

  SELECT
    coach_id,
    COUNT(DISTINCT member_id) AS open_member_count
  FROM coach_assignments
  WHERE ended_at IS NULL
    AND role = 'primary'
    AND member_id NOT IN (
      -- filter out any seats occupied by current QA members, i.e. Mock Coach members
      SELECT ta.member_id
      FROM track_assignments AS ta
      INNER JOIN tracks AS t ON ta.track_id = t.track_id
      WHERE ta.ended_at IS NULL
        AND t.deployment_type = 'qa'
    )
  GROUP BY coach_id

),

verticals_by_coach AS (

  SELECT
    -- aggregate verticals by coach
    coach_id,
    ARRAY_AGG(specialist_vertical_uuid) AS specialist_vertical_uuids,
    ARRAY_AGG(specialist_vertical_id) AS specialist_vertical_ids,
    ARRAY_AGG(specialist_vertical) AS specialist_verticals
  FROM coach_specialist_vertical
  GROUP BY coach_id

)


SELECT
  c.user_id AS coach_id,
  i.email,
  i.first_name,
  i.last_name,
  -- fountain_applicant_id data has not been fully backfilled
  p.fountain_applicant_id AS fnt_applicant_id,
  c.created_at,
  c.deactivated_at,
  p.staffable_state = 'staffable' AS staffable,
  p.staffable_state,
  sd.first_staffable_at,
  sd.last_staffable_at,
  c.time_zone,
  c.tz_iana,
  p.currency_code,
  CASE
    -- use currency to disambiguate time zone -> country mapping
    WHEN c.country_code = 'US' AND p.currency_code = 'CAD' THEN 'Canada'
    ELSE c.country_name
  END AS country_name,
  CASE
    -- use currency to disambiguate time zone -> country mapping
    WHEN c.country_code = 'US' AND p.currency_code = 'CAD' THEN 'CA'
    ELSE c.country_code
  END AS country_code,
  c.subregion_m49,
  c.geo,
  {{ priority_language('pl.ranked_language', 'pl.staffing_languages') }} AS priority_language,
  pl.staffing_languages,
  p.max_member_count,
  COALESCE(om.open_member_count, 0) AS seats_occupied_count,
  GREATEST(COALESCE(p.max_member_count, 0) - COALESCE(om.open_member_count, 0), 0) AS seats_available_count,
  COALESCE(om.open_member_count, 0) > COALESCE(p.max_member_count, 0) AS overstaffed,
  p.is_primary_coach as type_primary,
  p.is_on_demand_coach as type_on_demand,
  array_contains('pathways'::variant, p.staffing_qualifications) AS type_pathways,
  vc.coach_id IS NOT NULL AS type_extended_network,
  vc.specialist_vertical_uuids,
  vc.specialist_vertical_ids,
  vc.specialist_verticals,
  p.coaching_cloud,
  p.engaged_member_count,
  p.current_volunteer_member_count,
  p.segment_priority_level,
  p.coach_bio as bio,
  p.endorsement,
  p.experience_highlight,
  p.outlook,
  p.invoices_with_shortlist,
  p.last_book_read,
  p.coach_style_words,
  p.most_grateful_for,
  p.greatest_accomplishment,
  -- convert staffing attributes to lowercase and remove blanks
  {{string_to_array()}}(lower(array_to_string(p.staffing_qualifications, ';')), ';') AS staffing_qualifications,
  {{string_to_array()}}(lower(array_to_string(p.staffing_member_levels, ';')), ';') AS staffing_member_levels,
  lower(NULLIF(p.staffing_tier, '')) AS staffing_tier,
  {{string_to_array()}}(lower(array_to_string(p.staffing_industries, ';')), ';') AS staffing_industries,
  lower(NULLIF(p.staffing_risk_level, '')) AS staffing_risk_level
FROM users AS c
INNER JOIN user_info AS i ON c.user_id = i.user_id
INNER JOIN coach_profiles AS p ON c.user_id = p.coach_id
INNER JOIN languages_by_coach AS pl ON p.coach_id = pl.coach_id
LEFT OUTER JOIN verticals_by_coach AS vc ON c.user_id = vc.coach_id
LEFT OUTER JOIN open_members_by_coach AS om ON c.user_id = om.coach_id
LEFT OUTER JOIN staffable_dates as sd on p.coach_profile_uuid = sd.coach_profile_uuid
LEFT OUTER JOIN wpm2_mock_sessions
  ON i.email = wpm2_mock_sessions.coach_email
WHERE array_contains('coach'::variant, c.roles) = true
-- filter out QA coaches, and coaches in BU-related orgs:
-- Note that the app does not enforce that coaches have non-null organization_id
AND (c.organization_id IS NULL OR c.organization_id NOT IN (54, 1, 210))
AND NOT p.is_qa_coach
