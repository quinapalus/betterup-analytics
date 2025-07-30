WITH versions AS (
    SELECT * FROM {{ ref('base_app_centralized__versions') }}
)

SELECT * FROM versions

-- this versions table only has item_types for:

--'Coach::CoachProfileSolution'
--'Coach::ProfileIslandAttribute'
--'Coach::BasePayRate' use 'item_id' for joins
--'Coach::CoachQualification'
--'Coach::CoachProfilePayRate' use 'item_uuid' for joins
--'Coach::CoachProfile' use 'item_uuid' for joins
