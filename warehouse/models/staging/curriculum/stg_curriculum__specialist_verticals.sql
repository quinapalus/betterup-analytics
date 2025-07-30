WITH specialist_verticals AS (

  SELECT * FROM {{ source('app', 'specialist_verticals') }}

)

SELECT
        id AS specialist_vertical_id,  -- still functional but only for US data (we can comment out once coaching_varieties in coach_profiles is fixed)
        uuid AS specialist_vertical_uuid,
        {{ load_timestamp('created_at') }},
        {{ load_timestamp('updated_at') }},
        description,
        description_i18n,
        disclaimer,
        disclaimer_i18n,
        hero_image_url,
--        name,  --this column is mainly null and probably not used
        name_i18n,
        PARSE_JSON(name_i18n):en::VARCHAR AS name,
        resource_id,
        thumbnail_image_url,
        coach_matching_assessment_prompt_i18n,
        key,
        {{ environment_null_if('"ORDER"','"ORDER"')}},
        primary_at,
        post_session_configuration_id,
        post_session_configuration_uuid,
        coach_match_configuration_id,
        coach_match_configuration_uuid,
        coach_availability_period,
        share_coach_matching_assessment_with_coach

FROM specialist_verticals