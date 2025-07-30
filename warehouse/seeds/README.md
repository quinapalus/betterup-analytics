# dbt seed

The `csv` files in this `data` directory are designed to be loaded into the data warehouse using the [dbt seed](https://docs.getdbt.com/reference#seed) command. Per `dbt`'s documentation, `dbt seed` _is appropriate for loading static data which changes infrequently_.

`dbt seed` takes the same `--target` parameter as `dbt run`, and additionally takes an optional `--show` parameter that displays a sample of the loaded table. So to run `dbt seed` in production:

```bash
$ dbt seed --target prod --show
```

Per the configuration options we've specified in [`dbt_project.yml`](../dbt_project.yml), `dbt seed` will materialize all `.csv` files in the `data` directory as tables within a schema named `static`.

## Time Zone and Geo Mapping

Our two primary goals are: (1) convert the native Rails time zones in `app_users.time_zone` to a more broadly applicable format, and (2) provide a reasonable way to aggregate `users` into different geographic regions. While (1) is useful across all `user` roles, our use case for (2) will focus primarily on `coaches`.

If `naming` is one of the hard problems in software development, drawing regional lines feels like the equivalent in geopolitics. To provide a maintainable, fairly exhaustive mapping without reinventing the wheel, we leverage a handful of broadly recognized standards:
* First, we map the Rails time zones in `app_users` to IANA's `tz` nomenclature (also referred to as the Olson format), using the values defined in `rails_time_zones.csv`
* Then we use IANA's `zoneinfo` dataset to map IANA timezone to ISO 3166 country codes in the `iana_tz_info.csv` file, which then allows us to map ISO country code to the UN's sub-region (as defined in UN M.49) using the values defined in `m49_geoscheme.csv`.
* And finally, we define values in `bu_geo_categories.csv` that map the 17 UN sub-regions to one of `AMER`, `APAC`, or `EMEA`.

It's worth noting that the Rails time zones don't allow us to differentiate between US and Canada directly, e.g. the Rails timezone `Eastern Time (US & Canada)` is mapped to `America/New_York`, which is then mapped to `US`.

### Rails time_zones
[Documentation](https://api.rubyonrails.org/v5.2/classes/ActiveSupport/TimeZone.html), [source](https://github.com/rails/rails/blob/v5.2.0/activesupport/lib/active_support/values/time_zone.rb)

Table sample:

| tz_rails             |  tz_iana             |
| -------------------- | -------------------- |
| Darwin               | Australia/Darwin     |
| Bern                 | Europe/Zurich        |
| Hawaii               | Pacific/Honolulu     |
| International Dat... | Etc/GMT+12           |
| Caracas              | America/Caracas      |
| Port Moresby         | Pacific/Port_Moresby |
| Zurich               | Europe/Zurich        |
| Mountain Time (US... | America/Denver       |
| Mumbai               | Asia/Kolkata         |
| Nairobi              | Africa/Nairobi       |
| ...                  | ...                  |


### IANA tz info

[Documentation](https://www.iana.org/time-zones), [source](https://data.iana.org/time-zones/releases/tzdata2018e.tar.gz) (`zone.tab`)

Table sample:

| country_code_iso | tz_iana            |
| ---------------- | ------------------ |
| KI               | Pacific/Kiritimati |
| QA               | Asia/Qatar         |
| BR               | America/Maceio     |
| BH               | Asia/Bahrain       |
| TO               | Pacific/Tongatapu  |
| CO               | America/Bogota     |
| LR               | Africa/Monrovia    |
| CA               | America/Halifax    |
| RU               | Asia/Irkutsk       |
| NG               | Africa/Lagos       |
| ...              | ...                |


### M.49 geoscheme

[Documentation](https://unstats.un.org/unsd/methodology/m49/#geo-regions), [source](https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes/blob/v7.0/all/all.csv)

Table sample:

| country_name     | country_code_iso | region_m49 | subregion_m49        |
| ---------------- | ---------------- | ---------- | -------------------- |
| Lebanon          | LB               | Asia       | Western Asia         |
| Turkey           | TR               | Asia       | Western Asia         |
| Angola           | AO               | Africa     | Sub-Saharan Africa   |
| Christmas Island | CX               | Oceania    | Australia and New... |
| Slovenia         | SI               | Europe     | Southern Europe      |
| Pitcairn         | PN               | Oceania    | Polynesia            |
| Madagascar       | MG               | Africa     | Sub-Saharan Africa   |
| Montenegro       | ME               | Europe     | Southern Europe      |
| Solomon Islands  | SB               | Oceania    | Melanesia            |
| Lesotho          | LS               | Africa     | Sub-Saharan Africa   |
| ...              | ...              | ...        | ...                  |


### BU geo categories

Table sample:

| subregion_m49        | geo  |
| -------------------- | ---- |
| Melanesia            | APAC |
| Southern Asia        | APAC |
| Polynesia            | APAC |
| Central Asia         | EMEA |
| Australia and New... | APAC |
| Sub-Saharan Africa   | EMEA |
| Latin America and... | AMER |
| Northern Europe      | EMEA |
| Western Asia         | EMEA |
| Eastern Asia         | APAC |
| ...                  | ...  |
