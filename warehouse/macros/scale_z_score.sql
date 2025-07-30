{% macro scale_z_score(z_score) -%}

-- convert z-score into integer scale score with mean of 50 and SD=20
-- bounded between [5, 100]
-- mirror of app logic here https://github.com/betterup/betterup-app/blob/master/app/models/concerns/assessments/whole_person_subdimension_scores.rb

ROUND(GREATEST(5, LEAST(100, {{ z_score }} * 20 + 50)), 0)

{%- endmacro %}
