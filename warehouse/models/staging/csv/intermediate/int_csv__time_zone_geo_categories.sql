
with rails_time_zones as (

  select * from {{ ref('stg_csv__rails_time_zones') }}

),

iana_tz_info as (

  select * from {{ ref('stg_csv__iana_tz_info') }}

),

m49_geoscheme as (

  select * from {{ ref('stg_csv__m49_geoscheme') }}

),

bu_geo_categories as (

  select * from {{ ref('stg_csv__bu_geo_categories') }}

),

final as (

    select
      rtz.tz_rails as time_zone,
      rtz.tz_iana,
      m49.country_code_iso as country_code,
      m49.country_name,
      case
        -- map gmt+12 to polynesia
        when rtz.tz_rails = 'International Date Line West' then 'Polynesia'
        else m49.subregion_m49
      end as subregion_m49,
      case
        -- map gmt+12 to apac
        when rtz.tz_rails = 'International Date Line West' then 'APAC'
        else gc.geo
      end as geo
    from rails_time_zones as rtz
    left outer join iana_tz_info as tz on rtz.tz_iana = tz.tz_iana
    left outer join m49_geoscheme as m49 on tz.country_code_iso = m49.country_code_iso
    left outer join bu_geo_categories as gc on m49.subregion_m49 = gc.subregion_m49
)

select * from final