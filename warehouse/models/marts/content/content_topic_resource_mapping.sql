WITH
resource_development_topics AS (

  SELECT * FROM {{ ref('stg_app__resource_development_topics') }}

)


SELECT
    development_topic_id,
    array_agg(DISTINCT resource_id) WITHIN GROUP (ORDER BY resource_id ASC) AS resources_ids
FROM resource_development_topics
GROUP BY
    development_topic_id
ORDER BY
    development_topic_id
