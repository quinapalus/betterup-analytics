{{
  config(
    tags=['classification.c3_confidential','snapshot_daily']
  )
}}

{% snapshot snapshot_sfdc_leads %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at = 'uuid_ts',
        )
    }}

    select * from {{ source('salesforce', 'leads') }}

{% endsnapshot %}
