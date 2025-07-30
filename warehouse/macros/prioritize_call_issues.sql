{% macro prioritize_call_issues(call_issues) %}

-- Prioritize and extract the most important call issue when
-- a coach reports multiple call issues. This macro should be used
-- for coach post-session assessments after January 2020 product update.
-- Prioritization based on business needs as explained here:
-- https://betterup.atlassian.net/wiki/spaces/AN/pages/478707787/Updating+business+A+V+issues+data+logic+after+2020+product+updates

CASE
  WHEN CONTAINS({{ call_issues }}, 'connect_problem') = true
    THEN 'connect_problem'
  WHEN CONTAINS({{ call_issues }}, 'call_dropped') = true
    THEN 'call_dropped'
  WHEN CONTAINS({{ call_issues }}, 'no_video') = true
    THEN 'no_video'
  WHEN CONTAINS({{ call_issues }}, 'no_audio') = true
    THEN 'no_audio'
  WHEN CONTAINS({{ call_issues }}, 'connection_quality') = true
    THEN 'connection_quality'
  WHEN CONTAINS({{ call_issues }}, 'other') = true
    THEN 'other'
  ELSE NULL
END

{% endmacro %}
