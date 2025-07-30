with source as (

    select * from {{ source('gsheets_ops_analytics', 'is_partner_sat_final') }}

),

renamed as (

    select
        {{ dbt_utils.surrogate_key(['launch_request_id', 'survey_sent_at', 'response_received', 'response_at']) }} as unique_id,
        expectation_satisfaction,
        how_to_improve,
        ipm,
        launch_request_id,
        member_success_satisfaction,
        overall_rating,
        resource_satisfaction,
        response_at,
        response_received,
        survey_sent_at,
        technical_satisfaction,
        what_did_you_like

    from source

)

select * from renamed
