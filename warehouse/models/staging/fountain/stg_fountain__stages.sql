with funnels as (

    select *  from {{ ref('snapshot_fountain_funnels') }}

)

select 
    funnels.id as funnel_id,
    parsed_stage.value:"id"::string as stage_id,
    parsed_stage.value:"title"::string as stage_title,
    parsed_stage.value:"type"::string as stage_type,
    --'Rejected','On Hold','Inactive' are the only stage buckets that cannot conform to the bucket naming convention. These are default fountain stages and cannot be renamed
    iff(stage_title not in ('Rejected','On Hold','Inactive'),REGEXP_SUBSTR(stage_title, '^[^ ]*'),stage_title) as stage_name_prefix,
    regexp_replace(replace(lower(replace(stage_title, ' - ', '_')), ' ', '_'), '[^a-z0-9_]', '') as stage_title_cleaned

from 
  funnels,
  /*
  the raw stages column from fountain comes in with the entire thing enclosed in double quotes 
  --and None wrapped in single quotes. This is not valid json. The code below cleans it up so that we
  --can treat it like a regular variant field
  */
  lateral flatten(input => parse_json(replace(replace(to_json(stages),'"',''),'None','"None"'))) as parsed_stage

--only need current state of stages
where dbt_valid_to is null 
