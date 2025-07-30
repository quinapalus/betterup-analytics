{% macro sanitize_wpm_model_version(questions_version) -%}

-- define user-friendly naming conventions for WPM assessments
-- Note: we're assuming that questions_version is NULL if assessment is still on 1.0 only,
-- and has values in 1.0/2.0 if the assessment has been migrated. We're checking these
-- assumption in corresponding tests on questions_version in app/base/app_assessments.yml

CONCAT('WPM ', COALESCE({{ questions_version }}, '1.0'))

{%- endmacro %}
