with experiment_assignment as (
    select * from {{ ref('int_amplitude__events')}}
    where true
        and event_type = '[Experiment] Assignment'
)

select * from experiment_assignment