{{
    config(
        tags=["snapshot_daily"]
    )
}}

{% snapshot snapshot_app__coach_assignments %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at='updated_at',
        )
    }}

    select * from {{ source('app', 'coach_assignments') }}

{% endsnapshot %}