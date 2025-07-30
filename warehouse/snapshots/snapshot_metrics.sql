{{
  config(
    tags=['snapshot_daily']
  )
}}

{% snapshot snapshot_metrics %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          check_cols= ['metric_value'],
          unique_key='primary_key'
        )
    }}

    select * from {{ ref('int_metrics__unioned') }} 

{% endsnapshot %}
