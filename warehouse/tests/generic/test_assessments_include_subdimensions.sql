
{% test assessments_include_subdimensions(model, subdimensions, assessment_types, lookback_days=3, threshold=0.05) %}

-- Filters the assessment item responses down to the desired assessment types.

with trimmed_assessment_subdimensions as 
(
    select
        m.assessment_id,
        m.submitted_at,
        m.assessment_type,
        m.subdimension_key 
    from {{ model }} m
    where 
        ( 
            {% for a_type in assessment_types %} 
                m.assessment_type = '{{a_type}}'
                {% if not loop.last %} or {% endif %} 
            {% endfor %}
        ) and 
        m.submitted_at >= dateadd('days', -{{lookback_days}}, current_timestamp)
    order by m.submitted_at desc
),

-- Uses the above subdimension score keys to pivot the item responses table and count existence.

pivot_subdimensions as 
(
    select 
        assessment_id,
        assessment_type,
        {% for subdim in subdimensions %}
            sum(
                case when trimmed_assessment_subdimensions.subdimension_key like '{{ subdim }}'
                    then 1 else 0
                end
            )
            as "{{ subdim }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    from trimmed_assessment_subdimensions
    group by assessment_id, assessment_type
),

-- Unpivot the above table so we can sum over all subdimension score counts. 
-- Since the `with` table is not a Relation, we will unpack this manually via macro unfolding.
-- The resulting table has two columns: the assessment id, and the number of missing subdimensions in that assessment.
-- The number of missing subdimensions is equal to the total number of subdimensions minus 1 for each subdimension found in the above pivot table.

assessments_missing_subdimensions as 
(
    select
        assessment_id,
        assessment_type,
        {{ subdimensions|length }}
        {% for k in subdimensions %} - "{{k}}" {% endfor %}
        as error_count 
    from pivot_subdimensions
),

-- Ensure the proportion of assessments with errors is low.

proportion_of_assessments_with_errors as 
(
    select 
        sum(case when error_count > 0 then 1 else 0 end) as with_errors,
        count(*) as total
    from assessments_missing_subdimensions
)

select * from proportion_of_assessments_with_errors
where cast(with_errors as float) / cast(total as float) > {{ threshold }}

{% endtest %}