WITH versions AS (
    SELECT *
    FROM {{ ref('base_app__versions_delete') }}
    )

SELECT * EXCLUDE(created_at), created_at AS destroyed_at
FROM versions