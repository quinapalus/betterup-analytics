{% test items_in_assessments_of_type(model, item_keys, assessment_types, lookback_days=3, threshold=0.05) %}

-- Trims the assessment item response set to the desired lookback window and the desired assessment types.

with trimmed_assessment_item_responses as 
(
    select
        m.assessment_id,
        m.submitted_at,
        m.assessment_type,
        m.item_key 
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

-- Pivots the assessment item response set into a table that indicates whether or not each item has a corresponding response in that assessment.
-- If an item is present, the value is 1.

pivot_item_keys as 
(
    select 
        assessment_id,
        assessment_type,
        {% for item_key in item_keys %}
            sum(
                case when trimmed_assessment_item_responses.item_key like '{{ item_key }}'
                    then 1 else 0
                end
            )
            as "{{ item_key }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    from trimmed_assessment_item_responses 
    group by assessment_id, assessment_type
),

-- Unpivot the above table so we can sum over all item_key counts. 
-- Since the `with` table is not a Relation, we will unpack this manually via macro unfolding.
-- The resulting table has two columns: the assessment id, and the number of missing items in that assessment.
-- The number of missing items is equal to the total number of items minus 1 for each item found in the above pivot table.

assessments_missing_critical_items as 
(
    select 
        assessment_id,
        assessment_type,
        {{ item_keys|length }} 
        {% for k in item_keys %} - "{{k}}" {% endfor %} 
        as error_count
    from pivot_item_keys
),

-- Ensure the proportion of assessments with errors is low.

proportion_of_assessments_with_errors as 
(
    select 
        sum(case when error_count > 0 then 1 else 0 end) as with_errors,
        count(*) as total
    from assessments_missing_critical_items
)

select * from proportion_of_assessments_with_errors 
where cast(with_errors as float) / cast(total as float) > {{ threshold }}

{% endtest %}