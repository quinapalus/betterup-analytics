{{
  config(
      tags=["snapshot_daily"]
  )
}}

{% snapshot snapshot_coach__profile_island_attributes %}

  {{
      config(
        target_schema='snapshots',
        strategy='timestamp',
        unique_key='uuid',
        updated_at='updated_at',
      )
  }}

  select * from {{ source('coach', 'profile_island_attributes') }}

{% endsnapshot %}
