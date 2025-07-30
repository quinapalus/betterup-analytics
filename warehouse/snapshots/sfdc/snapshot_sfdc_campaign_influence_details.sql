{{
  config(
    tags=['classification.c3_confidential','snapshot_daily']
  )
}}

{% snapshot snapshot_sfdc_campaign_influence_details %}

   {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at = 'uuid_ts',
        )
    }}

    select * from {{ source('salesforce', 'campaign_influence_detail') }}

{% endsnapshot %}
