with flattened as (

{{ flatten_json(
    model_name = source('salesforce','contacts'),
    json_column = '_airbyte_data',
    rename_airbyte_sfdc_column = true
) }} )

select
*
from flattened 
 --this model will return all of the columns in a format that (mostly)
 --matches the format of segment raw tables. 
