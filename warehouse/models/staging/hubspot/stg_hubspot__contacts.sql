with source as (
  select * from {{ source('hubspot', 'contacts') }}
),

renamed as (
select 
    --primary key
    id as hubspot_contact_id,

    --foreign keys
    portal_id as hubspot_portal_id,
    canonical_vid as hubspot_canonical_vid,
    lead_guid as hubspot_lead_guid,
    merged_vids as hubspot_merged_vids,

    --attributes
    email,
    is_contact,
    properties_lastname_value as contact_last_name,
    properties_firstname_value as contact_first_name,
    form_submissions,
    split(form_submissions, ',') as form_submission_array,
    array_size(form_submission_array) as current_count_of_form_submissions,
    list_memberships,
    split(list_memberships, ',') as list_membership_array,
    array_size(list_membership_array) as current_count_of_list_memberships,
    properties_company_value as contact_company_name,

    ----timestamps
    added_at,
    received_at,
    uuid_ts,
    properties_lastmodifieddate_value::timestamp as last_modified_at,

    ----attribution/utm related params
    properties_hs_analytics_source_value as analytics_source_value,
    properties_utm_adsetid_value as utm_adset_id,
    properties_utm_adgroupid_value as utm_adgroup_id,
    properties_utm_creative_value as utm_creative,
    properties_utm_medium_value as utm_medium,
    properties_utm_source_value as utm_source,
    properties_utm_term_value as utm_term,
    properties_utm_campaignid_value as utm_campaign_id,
    properties_utm_campaign_value as utm_campaign,
    properties_utm_content_value as utm_content,
    lower(properties_hs_analytics_source_data_1_value) as analytics_source_data_value_1,
    lower(properties_hs_analytics_source_data_2_value) as analytics_source_data_value_2,


    --other marketing attributes
    properties_ft_utm_source_c_value as ft_utm_source,
    properties_lt_utm_source_c_value as lt_utm_source,

    properties_hs_analytics_first_url_value as analytics_first_url,
    properties_hs_analytics_last_url_value as analytics_last_url,
    properties_marketing_lead_type_value as marketing_lead_type,

    properties_hs_analytics_num_page_views_value as analytics_num_page_views,
    properties_hs_latest_source_data_1_value as latest_source_data_1,
    properties_hs_latest_source_data_2_value as latest_source_data_2,
    properties_hs_latest_source_value as latest_source,
    properties_createdate_value as created_date

from source
)

select * from renamed