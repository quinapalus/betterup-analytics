{{
    config(
        tags=["snapshot_daily"]
    )
}}

{% snapshot snapshot_coach__timeslots %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='updated_at',
        )
    }}

    select * from {{ source('app', 'timeslots') }}

{% endsnapshot %}