with flattened as (

{{ flatten_json(
    model_name = source('salesforce','opportunity_line_items'),
    json_column = '_airbyte_data',
    rename_airbyte_sfdc_column = true
) }} )

select
distinct *
from flattened
--there are two rows that are inexplicably exact duplicates. distinct statement is here to remove those dupes
 --this model will return all of the columns in a format that (mostly)
 --matches the format of segment raw tables. 
