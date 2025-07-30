 -- depends_on: {{ ref('raw_metrics__kpi_3') }}

with unpivoted as (

{{ dbt_utils.unpivot(
    ref('raw_metrics__kpi_3'), 
    cast_to='varchar', 
    exclude = ['first_primary_coaching_access_month','primary_key'], 
    field_name = 'metric_name',
    value_name = 'metric_value') }}
),

final as (

    select
        first_primary_coaching_access_month::date as metric_date, --first_primary_coaching_access_month comes in as timestamp so casting to date
        lower(metric_name) as metric_name,
        metric_value::number as metric_value,
        'monthly' as metric_granularity
    from unpivoted)

select 
    {{ dbt_utils.surrogate_key(['metric_name', 'metric_date', 'metric_granularity']) }} as primary_key,
    final.*
from final
