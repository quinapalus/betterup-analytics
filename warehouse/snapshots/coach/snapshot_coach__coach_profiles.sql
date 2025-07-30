{{
  config(
      tags=["snapshot_daily"]
  )
}}

{% snapshot snapshot_coach__coach_profiles %}

  {{
      config(
        target_schema='snapshots',
        strategy='timestamp',
        unique_key='uuid',
        updated_at='updated_at',
      )
  }}

  select * from {{ source('coach', 'coach_profiles') }}

{% endsnapshot %}
