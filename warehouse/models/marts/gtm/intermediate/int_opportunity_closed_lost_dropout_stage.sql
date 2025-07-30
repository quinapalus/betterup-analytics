with opportunity_field_history as (

  select * from {{ ref('stg_sfdc__opportunity_field_history') }}
  where not is_deleted

),

opportunities_current_state as (

  select * from {{ ref('int_sfdc__opportunities_snapshot') }}
  where not is_deleted and is_current_version

)

select
    opportunity_field_history.sfdc_opportunity_id,
    {{ sanitize_opportunity_stage('opportunity_field_history.old_value') }} as closed_lost_dropout_stage
from opportunity_field_history
inner join opportunities_current_state
  on opportunities_current_state.sfdc_opportunity_id = opportunity_field_history.sfdc_opportunity_id
     and opportunity_stage = 'Closed Lost' --only opportunities that are currently closed lost should have a closed_lost_dropout_stage
where 
    field = 'StageName'
    and new_value in ('Closed Lost','7- Closed Lost')
qualify row_number() over(partition by opportunity_field_history.sfdc_opportunity_id order by opportunity_field_history.created_at) = 1
