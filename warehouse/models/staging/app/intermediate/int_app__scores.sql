{{
 config(
   tags=["eu"]
 )
}}

with scores as (

   select * from {{ ref('stg_app__scores') }}

),

destroyed_records as (
   select *
   from {{ ref('stg_app__versions_delete') }}
   where item_type = 'Score'
),

final as (

   select
       s.*
   from scores s
   left join destroyed_records
       on s.score_id = destroyed_records.item_id
   where destroyed_records.item_id is null

)

select * from final

