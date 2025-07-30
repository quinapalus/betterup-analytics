--we put all raw_metric models in the below list.
--the int_ models in this list are "legacy". Going forwared we are doing everything in the raw_metrics__ models

{%- set raw_metric_models = [
'int_metrics__kpi_1',
'int_metrics__kpi_2_activated',
'int_metrics__kpi_2_invited',
'int_metrics__kpi_3'
] %}

/*
unioning together all of the models in the raw_metric_models list
ideally we would use dbt_utils.union_relations macro 
but there are some issues with that macro not creating reference objects correctly
leading to missing dependcies at model runtime
*/

{%- for metric in raw_metric_models -%}
select
    {{ dbt_utils.surrogate_key(['metric_name', 'metric_date', 'metric_granularity']) }} as primary_key,
    metric_name,
    metric_date,
    metric_granularity,
    metric_value
from {{ ref(metric) }}
{% if not loop.last %} union all {% endif %}
{%- endfor -%}
