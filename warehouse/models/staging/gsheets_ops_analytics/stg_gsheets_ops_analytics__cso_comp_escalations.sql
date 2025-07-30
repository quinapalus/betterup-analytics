--this needs to be used also in EU instance

{{
  config(
    tags=["eu"]
  )
}}

WITH src_cso_comp_escalations AS (
    SELECT * FROM {{ source('gsheets_ops_analytics', 'cso_comp_escalations') }}
),
cso_comp_escalations AS (
    SELECT
        {{ dbt_utils.surrogate_key(['coach_assignment_id', 'member_id']) }} as coach_assignment_member_key
        , coach_assignment_id::int AS coach_assignment_id
        , coach_id::int AS coach_id
        , cso_ticket_link
        , cso_user_name
        , member_id
        , reason
        , submitted_date::timestamp_ntz AS submitted_at
    FROM src_cso_comp_escalations
)

SELECT * FROM cso_comp_escalations
