with hubspot_contacts as (
    select * from {{ ref('stg_hubspot__contacts')}}
    --hubspot form submissions are not unique in the array, therefore an additional step is needed to ensure that they are 
    --before performing a lateral flatten.
),

form_submissions as (
    select * from {{ ref('stg_hubspot__form_submissions')}}
),

hubspot_contacts__unique_form_submissions as (
    select
        hubspot_contact_id,

        array_distinct(form_submission_array) as unique_form_submission_array
    from hubspot_contacts
),

contact_form_submissions_flattened as (
    select
        hubspot_contacts__unique_form_submissions.hubspot_contact_id,
        hubspot_contacts__unique_form_submissions.unique_form_submission_array,

        --flattened
        flat.value::string as form_submission_id

    from hubspot_contacts__unique_form_submissions,
        lateral flatten(input => unique_form_submission_array) as flat
),

joined as (
    select
        form_submissions.*,
        contact_form_submissions_flattened.hubspot_contact_id
    from form_submissions
    left join contact_form_submissions_flattened
        on form_submissions.hubspot_form_submission_id = contact_form_submissions_flattened.form_submission_id
),

subset as (
    select
        {{ dbt_utils.surrogate_key(['hubspot_form_submission_id', 'hubspot_contact_id'])}} as _unique,
        *
    from joined
)

select * from subset