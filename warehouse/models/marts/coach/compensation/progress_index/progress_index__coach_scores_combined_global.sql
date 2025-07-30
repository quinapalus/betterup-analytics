{{ config(
    tags=["eu"],
    schema="coach"
) }}

with coach_scores as (
    select * from {{ ref('progress_index__coach_scores_global') }}
),
coach_item_scores as (
    select * from {{ ref('progress_index__coach_item_scores_global') }}
),
coach_profiles as (
    select * from {{ ref('int_coach__coach_profiles') }}
),
coach_scores_combined as (
    select
        coach_profiles.coach_id
        , coach_scores.coach_uuid
        , coach_scores.rescaled_coach_composite_score
        , coach_item_scores.rescaled_coach_overcome_obstacles_score
        , coach_item_scores.rescaled_coach_goal_progress_score
        , coach_item_scores.rescaled_coach_self_awareness_score
        , coach_item_scores.rescaled_coach_equiped_changes_score
        , coach_item_scores.rescaled_coach_impact_development_score
    from coach_profiles
    left join coach_scores
        on coach_scores.coach_uuid = coach_profiles.user_uuid
    left join coach_item_scores
        on coach_item_scores.coach_uuid = coach_profiles.user_uuid
)

select * from coach_scores_combined
