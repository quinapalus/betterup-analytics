{{
  config(
    tags=['classification.c3_confidential']
  )
}}
{% snapshot snapshot_sfdc_forecasts %}
    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at = 'uuid_ts',
        )
    }}
    select * from {{ source('salesforce', 'my_forecasts') }}
{% endsnapshot %}
