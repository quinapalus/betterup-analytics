with source as (
    select * from {{ source('gsheets_marketing_attribution_sk_mapping', 'metadata_attribution') }}
),

renamed as (
    select
        --primary
        _unique,

        --attributes
        {{ environment_null_if('utm_source__de','utm_source__de')}},
        {{ environment_null_if('utm_source__st','utm_source__st')}},
        coalesce(utm_source__st, utm_source__de::string) as utm_source,
        utm_content::string as utm_content,
        utm_campaign::string as utm_campaign,
        utm_medium::string as utm_medium,
        channel_attribution,

        --foreign keys
    {{ dbt_utils.surrogate_key(['utm_source', 'utm_medium', 'utm_campaign', 'utm_content'])}} as gsheet_channel_attribution_sk

    from source
)

select * from renamed