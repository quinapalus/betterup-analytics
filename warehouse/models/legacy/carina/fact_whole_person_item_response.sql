{{
  config(
    tags=['classification.c3_confidential']
  )
}}

WITH individual_wpm_item_responses AS (

  SELECT * FROM {{ref('dbt_whole_person_item_response')}}

)

SELECT * FROM (

SELECT
  irc.*,
  {{ dbt_utils.surrogate_key(['irc.member_key', 'irc.date_key','irc.account_key','irc.deployment_key','irc.app_member_assessment_id',
                              'irc.app_creator_assessment_id','irc.assessment_name','irc.item_key','irc.item_response']) }}  as primary_key,
  irb.assessment_name AS baseline_assessment_name,
  irb.date_key AS baseline_assessment_date_key,
  irb.app_creator_submitted_at AS baseline_app_creator_submitted_at,
  {{ get_date_difference ('irb.app_creator_submitted_at', 'irc.app_creator_submitted_at') }} AS baseline_days_before_current,
  irb.creator_role AS baseline_creator_role,
  irb.item_response AS baseline_item_response,
  irl.assessment_name AS latest_assessment_name,
  irl.date_key AS latest_assessment_date_key,
  irl.app_creator_submitted_at AS latest_app_creator_submitted_at,
  {{ get_date_difference ('irl.app_creator_submitted_at', 'irc.app_creator_submitted_at') }} AS latest_days_before_current,
  irl.creator_role AS latest_creator_role,
  irl.item_response AS latest_item_response,
  ROW_NUMBER() OVER (
      PARTITION BY 
      /*this is not recommended and a surrogate_key should be created as a 
      primary key on the table to partition by */
        irc.member_key,
        irc.date_key,
        irc.account_key,
        irc.deployment_key,
        irc.app_member_assessment_id,
        irc.app_creator_assessment_id,
        irc.assessment_name,
        irc.creator_role,
        irc.app_creator_submitted_at,
        irc.item_key,
        irc.item_response        
      ORDER BY irc.member_key,
              irc.date_key,
              irc.account_key,
              irc.deployment_key,
              irc.app_member_assessment_id,
              irc.app_creator_assessment_id,
              irc.assessment_name,
              irc.creator_role,
              irc.app_creator_submitted_at,
              irc.item_key,
              irc.item_response, 
              irb.app_creator_submitted_at ASC, 
              irl.app_creator_submitted_at DESC
  ) AS index
FROM individual_wpm_item_responses AS irc
-- join current to baseline response ordered by ascending date
LEFT OUTER JOIN individual_wpm_item_responses AS irb
  ON irc.member_key = irb.member_key
  AND irc.account_key = irb.account_key
  AND irc.item_key = irb.item_key
  AND irb.app_creator_submitted_at < irc.app_creator_submitted_at
  -- only compare to self-reported assessments
  AND irb.creator_role = 'self'
-- join current to latest response (relative to current response) ordered by descending date
LEFT OUTER JOIN individual_wpm_item_responses AS irl
  ON irc.member_key = irl.member_key
  AND irc.account_key = irl.account_key
  AND irc.item_key = irl.item_key
  AND irl.app_creator_submitted_at < irc.app_creator_submitted_at
  AND irl.creator_role = 'self'

) a

WHERE index = 1
