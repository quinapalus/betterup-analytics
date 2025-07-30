{{
  config(
    tags=['classification.c2_restricted']
  )
}}

select

    --ids
    id as sfdc_campaign_member_id,
    {{ dbt_utils.surrogate_key(['id','dbt_valid_from','dbt_valid_to']) }} as history_primary_key,
    campaign_id as sfdc_campaign_id,
    lead_or_contact_id as sfdc_person_id,
    fcrm_fcr_opportunity_c as related_fcr_opportunity_id,

    --categorical and text attributes
    status as campaign_member_status,
    fcrm_fcr_response_status_c as response_status,
    fcsc_fcdsc_utm_campaign_c as utm_campaign,
    fcsc_fcdsc_utm_content_c as utm_content,
    fcsc_fcdsc_utm_medium_c as utm_medium,
    fcsc_fcdsc_utm_source_c as utm_source,
    fcsc_fcdsc_utm_term_c as utm_term,
    fcsc_fcdsc_referrer_page_c as referrer_page,
    mql_marketing_segment_c as mql_marketing_segment,

    --dates and timestamps
    {{ load_timestamp('all_campaign_members.created_date', alias='created_at') }},
    {{ load_timestamp('fcrm_fcr_response_date_c', alias='response_timestamp') }},
    coalesce(fcrm_fcr_inquiry_target_date_c,fcrm_fcr_qr_date_c) as inquiry_date,
    fcrm_fcr_qr_date_c as mql_date,
    {{ load_timestamp('mql_timestamp_c', alias='mql_timestamp') }},
    fcrm_fcr_sar_date_c as sal_date,
    {{ load_timestamp('sal_timestamp_c', alias='sal_timestamp') }},
    cfcr_fm_date_c as fm_date,
    {{ load_timestamp('fm_timestamp_c', alias='fm_timestamp') }},

    --booleans
    fcrm_fcr_inquiry_target_c as is_inquiry,
    fcrm_fcr_qr_c as is_mql,
    fcrm_fcr_sar_c as is_sal,
    cfcr_fm_c as is_fm,

    --other
    is_deleted,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current_version,
    row_number() over(
      partition by id
      order by dbt_valid_from
    ) as version,

    case when
      row_number() over(
        partition by id,date_trunc('day',dbt_valid_from)
        order by dbt_valid_from desc
      ) = 1 then true else false end as is_last_snapshot_of_day

from {{ ref('snapshot_sfdc_campaign_members') }} as all_campaign_members

--this join and where clause is a temporary solution to remove hard deletes from downstream models.
--see https://betterup.atlassian.net/browse/DATA-1661

left join {{ source('salesforce_hard_deletes', 'campaign_members') }} as hard_deleted_campaign_members
  on hard_deleted_campaign_members.sfdc_record_id = all_campaign_members.id
where hard_deleted_campaign_members.sfdc_record_id is null
