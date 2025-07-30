-- Each feature below is a dimension that contains an array with data
{% set features = [
    'account_qualifications', 'staffing_industries', 'coaching_cloud',
    'additional_qualifications', 'focus_qualifications', 'product_qualifications',
    'segment_qualifications', 'professional_qualifications', 'postgrad_qualifications',
    'staffing_member_levels', 'race_ethnicity', 'coaching_varieties', 'group_coaching_qualifications',
    'certification_qualifications']
%}

WITH coach_profiles AS (
    SELECT * FROM {{ ref('int_coach__coach_profiles') }}
),
language_names AS (
    SELECT * FROM {{ ref('iso_639_language_codes') }} --csv file under seeds (language acronyms to language full name)
),
languages_feature AS (
-- fans-out the array into one row per features
-- this needs to be done for this particular feature so that we can mapp the full language names below

    SELECT
         coach_profile_uuid
        , coach_profile_id
        , 'staffing_languages' AS feature_type
        , value::string AS feature_key
    FROM coach_profiles,
         LATERAL FLATTEN(input => staffing_languages)
),
languages_mapped AS (
-- just joining the csv file containing full language names

    SELECT
         coach_profile_uuid
        , coach_profile_id
        , feature_type
        , COALESCE(language_name, feature_key) AS feature_key -- use full language name instead of acronym
    FROM languages_feature
    LEFT JOIN language_names
        ON language_names.alpha2 = languages_feature.feature_key
),
features AS (
-- this is a jinja for-loop that iterates through each feature and fans-out the array into
-- one row per feature, and then UNIONS all the features together into one view

    {% for feature in features -%}

        SELECT
             coach_profile_uuid
            , coach_profile_id
            , '{{ feature }}' AS feature_type
            , value::string AS feature_key
        FROM coach_profiles,
             LATERAL FLATTEN(input => {{ feature }} )

    {% if not loop.last -%} UNION ALL {%- endif %}

    {% endfor -%}

    UNION ALL

    SELECT
         coach_profile_uuid
        , coach_profile_id
        , feature_type
        , feature_key
    FROM languages_mapped
),
final AS (

    SELECT
        {{ dbt_utils.surrogate_key(['coach_profile_uuid', 'feature_type', 'feature_key']) }} AS coach_profile_feature_id
        , coach_profile_id
        , coach_profile_uuid
        , feature_type
        , feature_key
    FROM features
    WHERE feature_key IS NOT NULL
      AND feature_type IS NOT NULL
)

SELECT * FROM final
