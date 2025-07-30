WITH reporting_group_organizations AS (
    SELECT * FROM {{ ref('stg_app__reporting_group_organizations') }}
),
destroyed_records AS (
    SELECT * FROM {{ref('stg_app__versions_delete')}}
    WHERE item_type = 'ReportingGroupOrganization'
),
final AS (

    SELECT
        rgo.reporting_group_organization_id,
        rgo.reporting_group_id,
        rgo.organization_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }}
    FROM reporting_group_organizations AS rgo
    LEFT JOIN destroyed_records AS v ON rgo.reporting_group_organization_id = v.item_id
    WHERE v.item_id IS NULL
)

SELECT * FROM final
