{{
  config(
    tags=['eu']
  )
}}

WITH applicants AS (
  SELECT * FROM {{ source('fountain', 'applicants') }}
)

SELECT
  id AS fountain_applicant_id,
  first_name,
  last_name,
  lower(email) AS primary_email,
  lower(personal_email) AS secondary_email,
  phone_number AS phone,
  funnel,
  stage,
  created_at,
  last_transitioned_at,
  receive_automated_emails,
  inspiring_matrix,
  inspiring_application_tier,
  thriving_matrix,
  thriving_application_tier,
  coach_interview_tier,
  coaching_credential,
  additional_certifications,
  vip_status,
  high_priority,
  CASE WHEN city_custom IS NULL
    THEN
      CASE WHEN city IS NULL THEN location ELSE city END
    ELSE city_custom END AS city,
  CASE WHEN country_custom IS NULL THEN country ELSE country_custom END AS country,
  green_state,
  time_zone,
  CASE WHEN referrer IS NULL THEN referrer_name ELSE referrer END AS referrer,
  previous_applicant,
  CASE WHEN how_did_they_hear_about_betterup IS NULL
    THEN where_did_you_find_out_about_this_opportunity
    ELSE how_did_they_hear_about_betterup END AS candidate_pipeline,
  statement_of_interest,
  coaching_background_and_clients,
  describe_coaching_style_in_three_words,
  explain_coaching_to_a_new_client,
  how_would_you_build_rapport_with_clients,
  how_do_you_handle_disengaged_clients,
  how_do_you_stay_updated_on_evidence_based_practices,
  tell_us_something_youve_learned_recently,
  anything_else,
  PARSE_JSON(coaching_languages) AS coaching_languages,
  adv_deg_tier,
  postgraduate_degrees,
  postgraduate_degree_field,
  current_coursework,
  completed_courses,
  semesters_of_graduate_study,
  coach_cert_tier,
  coach_training_institute,
  coaching_certifications,
  non_icf_cert,
  coach_training_completed,
  CASE WHEN coach_training_program IS NULL
    THEN coach_training_institute
    ELSE coach_training_program END AS coach_training_program,
  coach_hrs_tier,
  professional_coaching_hours,
  counseling_hrs_tier,
  supervised_clinical_hours,
  professional_license_type,
  member_level,
  corporate_or_professional_experience,
  coaching_techniques,
  coaching_industries,
  working_industries,
  focus_area,
  mbti_certified
FROM applicants