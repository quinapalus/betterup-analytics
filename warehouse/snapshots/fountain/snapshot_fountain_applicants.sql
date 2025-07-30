{% snapshot snapshot_fountain_applicants %}

    {{
        config(
          target_schema='snapshots',
          strategy='timestamp',
          unique_key='id',
          updated_at = 'updated_at',
        )
    }}

    select * from {{ source('fountain', 'applicants') }}

{% endsnapshot %}
