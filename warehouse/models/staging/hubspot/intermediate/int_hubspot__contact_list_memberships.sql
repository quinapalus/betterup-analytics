with hubspot_contacts as (
    select * from {{ ref('stg_hubspot__contacts')}}
    --hubspot contact list memberships are not unique in the array, therefore an additional step is needed to ensure that they are 
    --before performing a lateral flatten.
),

contact_lists as (
    select * from {{ ref('stg_hubspot__contact_lists')}}
),

hubspot_contacts__unique_list_memberships as (
    select
        hubspot_contact_id,

        array_distinct(list_membership_array) as unique_list_memberships_array
    from hubspot_contacts
),

contact_list_memberships_flattened as (
    select
        hubspot_contacts__unique_list_memberships.hubspot_contact_id,
        hubspot_contacts__unique_list_memberships.unique_list_memberships_array,

        --flattened
        flat.value::string as list_membership_id

    from hubspot_contacts__unique_list_memberships,
        lateral flatten(input => unique_list_memberships_array) as flat
),

joined as (
    select
        contact_lists.*,
        contact_list_memberships_flattened.hubspot_contact_id
    from contact_lists
    left join contact_list_memberships_flattened
        on contact_lists.hubspot_contact_list_id = contact_list_memberships_flattened.list_membership_id
),

subset as (
    select
        {{ dbt_utils.surrogate_key(['hubspot_contact_list_id', 'hubspot_contact_id'])}} as _unique,
        *
    from joined
)

select * from subset