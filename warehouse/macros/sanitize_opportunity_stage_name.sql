{% macro sanitize_opportunity_stage(opportunity_stage) -%}

case 
 
    when
        {{ opportunity_stage }} = '0 - First Meeting'
    then '0-First Meeting'

    when
        {{ opportunity_stage }} = '3 - Solutioning'
    then '3-Solutioning'

    when
        {{ opportunity_stage }} = '4 - Closing in'
    then '4-Closing in'

    when
        {{ opportunity_stage }} = '5 - Verbal'
    then '5-Verbal'

    when
        {{ opportunity_stage }} = '6 - Contracting'
    then '6-Contracting'

    when
        {{ opportunity_stage }} = '7- Closed Won'
    then 'Closed Won'

    when
        {{ opportunity_stage }} = '7- Closed Lost'
    then 'Closed Lost'

    else {{ opportunity_stage }} end
{%- endmacro %}
