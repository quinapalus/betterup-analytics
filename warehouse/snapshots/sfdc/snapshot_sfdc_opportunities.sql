{{
  config(
    tags=['classification.c3_confidential','snapshot_daily']
  )
}}

{% snapshot snapshot_sfdc_opportunities %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
           updated_at = 'updated_at_config'
        )
    }}

  select 
    *,
    {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
        last_modified_date as updated_at_config
    {% else %} 
        uuid_ts as updated_at_config
    {% endif %}
    from {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %} 
            {{ ref('base_fed_sfdc__opportunities') }} 
          {% else %} 
            {{ source('salesforce', 'opportunities') }}
          {% endif %}

{% endsnapshot %}
