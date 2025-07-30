WITH rails_time_zones AS (
  SELECT * FROM {{ ref('rails_time_zones') }} --csv file
),
iana_tz_info AS (
  SELECT * FROM {{ ref('iana_tz_info') }} --csv file
),
m49_geoscheme AS (
  SELECT * FROM {{ ref('m49_geoscheme') }} --csv file
),
bu_geo_categories AS (
  SELECT * FROM {{ ref('bu_geo_categories') }} --csv file
),
final AS (
    SELECT
      rtz.tz_rails AS time_zone -- use to join on users.time_zone
      , rtz.tz_iana AS time_zone_iana
      , m49.country_code_iso AS country_code
      , m49.country_name AS country_name
      , m49.region_m49 AS region
      , CASE -- map GMT+12 to Polynesia
          WHEN rtz.tz_rails = 'International Date Line West' THEN 'Polynesia'
          ELSE m49.subregion_m49
        END AS subregion
      , CASE -- map GMT+12 to APAC
          WHEN rtz.tz_rails = 'International Date Line West' THEN 'APAC'
          ELSE geo_cat.geo
        END AS geo
    FROM rails_time_zones AS rtz
    LEFT OUTER JOIN iana_tz_info AS iana_tz ON rtz.tz_iana = iana_tz.tz_iana
    LEFT OUTER JOIN m49_geoscheme AS m49 ON iana_tz.country_code_iso = m49.country_code_iso
    LEFT OUTER JOIN bu_geo_categories AS geo_cat ON m49.subregion_m49 = geo_cat.subregion_m49
)

SELECT * FROM final
