WITH versions AS (
    SELECT *
    FROM {{ ref('base_app__versions_create') }}
    )

SELECT *
FROM versions