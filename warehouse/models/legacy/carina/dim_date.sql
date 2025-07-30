WITH date_spine AS (

  SELECT
    date_day::date as date,
    EXTRACT(year FROM {{dateadd('month', -1, 'date_day')}}) + 1 AS fiscal_year,
    'Q' || EXTRACT(quarter FROM {{dateadd('month', -1, 'date_day')}}) AS fiscal_quarter

  FROM {{datespine(
            datepart = "day",
            start_date = "'2015-02-09'::timestamp",
            end_date = dateadd('year', 5, 'current_date')
        )}}

),

current_quarter AS (

  SELECT
    fiscal_year,
    fiscal_quarter
  FROM date_spine
  WHERE date = current_date
  LIMIT 1

),

previous_quarter AS (

  SELECT
    fiscal_year,
    fiscal_quarter
  FROM date_spine
  WHERE date = {{dateadd('month', -3, 'current_date')}}
  LIMIT 1

)


SELECT
  {{ date_key('d.date') }} AS date_key,
  d.date,
  to_char(d.date, 'YYYY-MM') AS calendar_year_month,
  {{full_month_name('d.date')}} AS calendar_month_name,
  to_char(d.date, 'MM') AS calendar_month_number,
  to_char(d.date, 'YYYY') AS calendar_year,
  d.fiscal_year,
  d.fiscal_quarter,
  d.fiscal_year || '-' || d.fiscal_quarter AS fiscal_year_quarter,
  d.fiscal_year = cq.fiscal_year AND d.fiscal_quarter = cq.fiscal_quarter
    AS is_current_fiscal_quarter,
  d.fiscal_year = pq.fiscal_year AND d.fiscal_quarter = pq.fiscal_quarter
    AS is_previous_fiscal_quarter
FROM date_spine AS d
CROSS JOIN current_quarter AS cq
CROSS JOIN previous_quarter AS pq
