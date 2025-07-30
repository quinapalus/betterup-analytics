with hubspot_contacts as (
    select * from {{ ref('stg_hubspot__contacts')}}
),

subset_columns as (
    select
        --primary key
        {{ dbt_utils.surrogate_key(['hubspot_contact_id'])}} as marketing_lead_id,

        --foreign keys
        hubspot_contact_id,
        utm_adset_id,
        utm_adgroup_id,
        utm_campaign_id,

        --attributes
        email,
        contact_company_name,
        marketing_lead_type,

        ---aggregated facts
        current_count_of_form_submissions,
        current_count_of_list_memberships,

        --utm attribution attributes
        utm_creative,
        utm_medium,
        utm_source,
        utm_term,
        utm_content,

        -----timestamp attributes
        added_at,
        last_modified_at


    from hubspot_contacts
)

select * from subset_columns