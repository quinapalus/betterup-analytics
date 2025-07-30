WITH versions AS (
    SELECT *
    FROM {{ ref('base_app__versions_update') }}
    )

SELECT *
FROM versions