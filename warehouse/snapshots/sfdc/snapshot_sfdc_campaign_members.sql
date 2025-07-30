{{
  config(
    tags=['classification.c3_confidential','snapshot_daily']
  )
}}

{% snapshot snapshot_sfdc_campaign_members %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='id',
          check_cols = ['uuid_ts','mql_marketing_segment_c']
        )
    }}

    select * from {{ source('salesforce', 'campaign_members') }}

{% endsnapshot %}
