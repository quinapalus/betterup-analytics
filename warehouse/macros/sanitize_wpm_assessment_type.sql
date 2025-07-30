{% macro sanitize_wpm_assessment_type(assessment_type, questions_version) -%}

-- define user-friendly naming conventions for WPM assessments
-- Note: we're assuming that questions_version is NULL if assessment is still on 1.0 only,
-- and has values in 1.0/2.0 if the assessment has been migrated. We're checking these
-- assumption in corresponding tests on questions_version in app/base/app_assessments.yml

CONCAT(
  {{ sanitize_wpm_model_version(questions_version) }},
  ' ',
  CASE
    WHEN {{ assessment_type }} = 'Assessments::WholePersonAssessment' THEN 'Baseline'
    WHEN {{ assessment_type }} = 'Assessments::WholePersonProgramCheckinAssessment' THEN 'Reflection Point'
    WHEN {{ assessment_type }} = 'Assessments::WholePersonGroupCoachingCheckinAssessment' THEN 'Group Coaching Reflection Point'
    WHEN {{ assessment_type }} = 'Assessments::WholePerson360Assessment' THEN '360'
    WHEN {{ assessment_type }} = 'Assessments::WholePerson360ContributorAssessment' THEN '360'
    WHEN {{ assessment_type }} = 'Assessments::WholePerson180Assessment' THEN '180'
    WHEN {{ assessment_type }} = 'Assessments::WholePerson180ContributorAssessment' THEN '180'
  END
)

{%- endmacro %}
