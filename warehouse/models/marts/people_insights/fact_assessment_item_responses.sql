{{
  config(
    tags=['eu']
  )
}}

WITH assessments AS (

  SELECT * FROM {{ ref('stg_app__assessments') }}

),

item_responses AS (

  SELECT * FROM {{ ref('dim_item_responses') }}

),

wp_subdimensions AS (

  SELECT * FROM {{ ref('dim_whole_person_subdimension') }}

),

app_assessment_items AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_items')}}

),

assessment_item_responses AS (

    SELECT * FROM {{ ref('stg_app__assessment_item_responses')}}

),

legacy_assessment_items AS (

  SELECT * FROM {{ ref('legacy_assessment_items') }}

),

assessment_configurations AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_configurations')}}

),

assessment_configuration_sections AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_configuration_sections')}}

),

assessment_sections AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_sections')}}

),

assessment_section_items AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_section_items')}}

),

assessment_response_sets AS (

  SELECT * FROM {{ ref('int_people_insights__assessment_response_sets')}}

),

assessment_response_options AS (

  SELECT * FROM {{ ref('stg_assessment__assessment_response_options')}}

),

-- Path 1: Assessments with records in ASSESSMENT_ITEM_RESPONSES (assessments post 9/9/22)
path1 as (
    SELECT
        {{ dbt_utils.surrogate_key(['a.assessment_id', 'ai.assessment_item_uuid']) }} as primary_key,
        a.assessment_id,
        ai.key AS item_key,
        ir.item_response,
        JSON_EXTRACT_PATH_TEXT(ai.PROMPT_I18N, 'en') AS prompt,
        JSON_EXTRACT_PATH_TEXT(aro.LABEL_I18N, 'en') AS response_option_label,
        ai.assessment_item_id,
        ai.assessment_item_uuid,
        ars.key AS response_set_key,
        ars.item_type as response_set_type,
        ars.response_set_number_of_options,
        NULL as html_label
    FROM assessments AS a
    -- INNER JOIN so that we only capture Path 1 assessments
    INNER JOIN assessment_item_responses AS air
        ON a.assessment_id = air.assessment_id
    LEFT JOIN app_assessment_items AS ai
        ON air.assessment_item_uuid = ai.assessment_item_uuid
    LEFT JOIN assessment_response_sets AS ars
        ON ai.ASSESSMENT_RESPONSE_SET_ID = ars.assessment_response_set_id
    LEFT JOIN item_responses AS ir
        ON a.assessment_id = ir.assessment_id AND ai.key = ir.item_key
    LEFT JOIN assessment_response_options AS aro
        ON aro.ASSESSMENT_RESPONSE_SET_ID = ars.assessment_response_set_id and aro.VALUE = ir.ITEM_RESPONSE
),
-- Path 2: Assessment without records in ASSESSMENT_ITEM_RESPONSES (assessments pre 9/9/22)
-- uses the legacy gsheet (now a seed) data for assessment item metadata
path2_ir_without_air_values AS (
    SELECT ir.*
    FROM item_responses ir
    LEFT JOIN assessment_item_responses air
        ON ir.assessment_id = air.assessment_id
    WHERE air.assessment_item_response_id IS NULL
),

path2_first_cte AS (
    -- code-based assessments
    SELECT
        -- in the pre-ASSESSMENT_ITEM_RESPONSES world, we relied on item_key as (non-enforced) unique key.
        {{ dbt_utils.surrogate_key(['ir.assessment_id', 'ir.item_key']) }} as primary_key,
        ir.assessment_id,
        ir.item_key,
        ir.item_response,
        JSON_EXTRACT_PATH_TEXT(ai.PROMPT_I18N, 'en') AS prompt,
        JSON_EXTRACT_PATH_TEXT(aro.LABEL_I18N, 'en') AS response_option_label,
        ai.assessment_item_id,
        ai.assessment_item_uuid,
        ars.key AS response_set_key,
        ars.item_type as response_set_type,
        ars.response_set_number_of_options,
        lair.html_label
    FROM path2_ir_without_air_values AS ir
    LEFT OUTER JOIN legacy_assessment_items  AS lair
        ON ir.type = lair.assessment_type AND ir.item_key = lair.item_key
    LEFT OUTER JOIN app_assessment_items AS ai
        ON lair.item_uuid = ai.assessment_item_uuid
    LEFT JOIN assessment_response_sets AS ars
        ON ai.ASSESSMENT_RESPONSE_SET_ID = ars.assessment_response_set_id
    LEFT JOIN assessment_response_options AS aro
        ON ars.assessment_response_set_id = aro.ASSESSMENT_RESPONSE_SET_ID AND aro.VALUE = ir.ITEM_RESPONSE
    WHERE ir.assessment_configuration_uuid IS NULL
),

path2_second_cte AS (
    -- custom assessments
    SELECT
        {{ dbt_utils.surrogate_key(['ir.assessment_id', 'ai.assessment_item_uuid']) }} as primary_key,
        ir.assessment_id,
        ir.item_key,
        ir.item_response,
        JSON_EXTRACT_PATH_TEXT(ai.PROMPT_I18N, 'en') AS prompt,
        JSON_EXTRACT_PATH_TEXT(aro.LABEL_I18N, 'en') AS response_option_label,
        ai.assessment_item_id,
        ai.assessment_item_uuid,
        ars.key AS response_set_key,
        ars.item_type as response_set_type,
        ars.response_set_number_of_options,
        NULL as html_label
    FROM path2_ir_without_air_values AS ir
    JOIN assessment_configurations AS ac
        ON ir.ASSESSMENT_CONFIGURATION_UUID = ac.assessment_configuration_uuid
    JOIN assessment_configuration_sections AS acs
        ON ac.assessment_configuration_id = acs.ASSESSMENT_CONFIGURATION_ID
    JOIN assessment_sections AS a_s
        ON acs.ASSESSMENT_SECTION_ID = a_s.assessment_section_id
    JOIN assessment_section_items AS asi
        ON a_s.assessment_section_id = asi.ASSESSMENT_SECTION_ID
    JOIN app_assessment_items AS ai
        ON asi.ASSESSMENT_ITEM_ID = ai.assessment_item_id AND ir.item_key = ai.KEY
    LEFT JOIN assessment_response_sets AS ars
        ON ai.ASSESSMENT_RESPONSE_SET_ID = ars.assessment_response_set_id
    LEFT JOIN assessment_response_options AS aro
        ON aro.ASSESSMENT_RESPONSE_SET_ID = ars.assessment_response_set_id and aro.VALUE = ir.ITEM_RESPONSE
    -- we group because otherwise the assessment_section_items join could cause a row fanout.
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

unioned AS (
    SELECT *, 'path1' as cte_for_troubleshooting FROM path1
    UNION ALL
    SELECT *, 'path2.1' FROM path2_first_cte
    UNION ALL
    SELECT *, 'path2.2' FROM path2_second_cte
)

SELECT 
  *,
  --response set flags
  iff(response_set_key = 'recommend_5' and item_response in ('1','2','3','4','5'),true,false) as is_response_set_recommend_5,
  iff(response_set_key = 'improved_5' and item_response in ('1','2','3','4','5'),true,false) as is_response_set_improved_5,
  iff(response_set_key in ('agree_5','agreement') and item_response in ('1','2','3','4','5'),true,false) as is_response_set_agree_5,
  iff(response_set_key in ('agree_5','agreement') and item_response in ('4','5'),true,false) as is_response_set_agree_5_to,
  iff(response_set_key in ('likelihood_10','nps_10','nps'),true,false) as is_response_set_nps_10,

  case
    when is_response_set_agree_5 and item_response in ('4','5') then 'Agree'
    when is_response_set_agree_5 and item_response in ('1','2','3') then 'Disagree-Neutral'
    when is_response_set_nps_10 and item_response in ('9','10') then 'Promoter'
    when is_response_set_nps_10 and item_response in ('7','8') then 'Neutral'
    when is_response_set_nps_10 and item_response in ('0','1','2','3','4','5','6') then 'Detractor'
    else 'N/A' end as response_category,
  
  case
    when is_response_set_nps_10 and response_category = 'Promoter'
    then 1 else 0 end as nps_promoter_flag,
  
  case
    when is_response_set_nps_10 and response_category = 'Detractor'
    then 1 else 0 end as nps_detractor_flag

FROM unioned
