{{ config(
    tags=["identify_ai_metrics"],
) }}

WITH ini_iai_members AS (

    SELECT * FROM {{ref('int_iai_members')}}

),

reporting_group_assignments AS (
   SELECT * FROM {{ref('dim_reporting_group_assignments')}}
),

member_satisfaction AS  (

    SELECT
        iim.member_id,
        iim.reporting_group_id,
        avg(assessment_item_responses.item_response) as avg_use_of_time_rating
    FROM {{ref('fact_assessments')}}  AS assessments
    INNER JOIN  {{ref('fact_assessment_item_responses')}} AS assessment_item_responses
        ON assessments.assessment_id = assessment_item_responses.assessment_id AND
           assessments.assessment_name = 'Post-Session Member Survey' AND
           assessment_item_responses.item_key = 'session_was_valuable'
    INNER JOIN reporting_group_assignments AS rga
        ON rga.member_id = assessments.user_id AND
           assessments.submitted_at >= rga.starts_at AND
           (rga.ended_at IS NULL OR assessments.submitted_at < rga.ended_at)
    INNER JOIN ini_iai_members AS iim
        ON iim.member_id = rga.member_id AND
           iim.reporting_group_id = rga.reporting_group_id
    WHERE assessments.submitted_at BETWEEN DATEADD(DAY,-60,GETDATE()) AND GETDATE() 
    GROUP BY iim.reporting_group_id, iim.member_id

),

construct_scores AS (

    SELECT
        s.*,
        ROW_NUMBER() OVER (PARTITION BY s.member_id, s.reporting_group_id, s.construct_type, s.construct_key ORDER BY s.submitted_at DESC) AS score_reverse_sequence
    FROM {{ref('fact_reporting_group_construct_scores')}} AS s
    WHERE s.assessment_type = 'Assessments::WholePersonProgramCheckinAssessment' AND 
        s.construct_key = 'ex_index'

),

matched_reference_scores AS (

    SELECT
        s.*,
        ROW_NUMBER() OVER (PARTITION BY s.member_id, s.reporting_group_id, s.construct_type, s.construct_key ORDER BY s.submitted_at) AS score_sequence
    FROM {{ref('fact_reporting_group_construct_scores')}} AS s
    WHERE s.assessment_type  IN ('Assessments::WholePersonAssessment', 'Assessments::OnboardingAssessment', 'Assessments::PrimaryCoachingModalitySetupAssessment') AND
            s.construct_key = 'ex_index'

),

member_growth AS (

    SELECT
        construct_scores.member_id, 
        construct_scores.reporting_group_id,
        (construct_scores.scale_score - matched_reference_scores.scale_score) / (matched_reference_scores.scale_score) AS percent_growth_from_reference
    FROM construct_scores
    INNER JOIN matched_reference_scores 
        ON construct_scores.member_id = matched_reference_scores.member_id AND
            matched_reference_scores.submitted_at < construct_scores.submitted_at AND
            matched_reference_scores.reporting_group_id = construct_scores.reporting_group_id
    WHERE construct_scores.score_reverse_sequence = 1 AND 
        matched_reference_scores.score_sequence = 1

)

SELECT 
    {{ dbt_utils.surrogate_key(['iim.member_id', 'iim.reporting_group_id']) }} AS primary_key,
    iim.member_id,
    iim.reporting_group_id, 
    ms.avg_use_of_time_rating,
    mg.percent_growth_from_reference
FROM ini_iai_members AS iim
LEFT JOIN member_satisfaction AS ms
    ON iim.member_id = ms.member_id AND
        iim.reporting_group_id = ms.reporting_group_id
LEFT JOIN member_growth AS mg
    ON iim.member_id = mg.member_id AND
        iim.reporting_group_id = mg.reporting_group_id
