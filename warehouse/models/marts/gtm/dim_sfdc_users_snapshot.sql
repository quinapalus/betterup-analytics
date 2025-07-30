with users_history as (

    select * from {{ ref('int_sfdc__users_snapshots') }}

)

select 

  users_history.*
  
from users_history
