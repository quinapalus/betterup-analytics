{{
  config(
    tags=['snapshot_daily']
  )
}}

{% snapshot snapshot_account_health_overall_score %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          check_cols='all',
          unique_key='sfdc_account_id'
        )
    }}

    select * from {{ ref('int_account_health_overall_score') }} 

{% endsnapshot %}
