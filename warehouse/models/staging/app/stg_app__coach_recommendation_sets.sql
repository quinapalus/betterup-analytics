with source as (
    select * from {{ source('app_temp', 'coach_recommendation_sets') }}
),

renamed as (

    select
        id AS coach_recommendation_set_id,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        user_id AS member_id,
        assessment_id,
        {{ load_timestamp('expired_at') }},
        type,
        coaching_type,
        status,
        deferred,
        {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'US Gov' %}
                coach_recommender_response,
            {% elif env_var('DEPLOYMENT_ENVIRONMENT', '') == 'EU Prod' %}
                coach_recommender_response,
            {% else %}
                coalesce(coach_recommender_response__st, coach_recommender_response__va::varchar) AS coach_recommender_response,
            {% endif %}
        coach_recommender_response_status_code,
        coach_availability_attributes,
        languages,
        availability_check,
        override,
        {% if env_var('DEPLOYMENT_ENVIRONMENT', '') == 'EU Prod' %}
            NULL AS debrief360
        {% else %}
            debrief360
        {% endif %}

    from source

)

select * from renamed
