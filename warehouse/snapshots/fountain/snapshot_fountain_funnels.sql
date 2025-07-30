{% snapshot snapshot_fountain_funnels %}

    {{
        config(
          target_schema='snapshots',
          strategy='check',
          unique_key='id',
          check_cols = 'all',
        )
    }}

    select * from {{ source('fountain', 'funnels') }}

{% endsnapshot %}
