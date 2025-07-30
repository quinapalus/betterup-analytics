WITH assessment_items AS (

  SELECT * FROM {{ref('dei_assessment_items')}}

),

wpm_meta AS (

  SELECT * FROM {{ref('item_definition_whole_person')}}

),

wpm_assessments AS (

  SELECT * FROM assessment_items
  WHERE type IN ('Assessments::WholePersonAssessment', 'Assessments::WholePersonProgramCheckinAssessment') AND
        created_at > '2017-02-04' -- Prior to this date WPM was on a 7 point scale

),

response_count_per_wpm_assessment AS (

  SELECT
    assessment_id,
    COUNT(DISTINCT item_key) AS response_count
  FROM wpm_assessments
  WHERE item_key IN (SELECT item_key FROM wpm_meta) -- Focus only on the WPM questions
  GROUP BY assessment_id
  -- Avoid use of HAVING here due to slower materalization
)


SELECT
  assessment_id
FROM response_count_per_wpm_assessment
WHERE response_count = (SELECT COUNT(DISTINCT item_key) FROM wpm_meta) -- Select only WPM assessments that had all questions answered
