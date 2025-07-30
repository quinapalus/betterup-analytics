# Carina 0.1

[Carina](https://en.wikipedia.org/wiki/Carina_(constellation)) is a conventional star-schema, based on Kimball Dimensional Modeling as described in [The Data Warehouse Toolkit](https://www.amazon.com/Data-Warehouse-Toolkit-Definitive-Dimensional/dp/1118530802). Carina is designed to serve as the presentation layer between the warehouse and non-SQL based BI tools (e.g. Domo).

## Facts vs Dimensions

As a general rule, Dimensions are used to filter or group data (think: filters, row labels, and column headings in pivot tables). Facts are numerical values used as input to aggregate functions.

## Conventions

Work-in-progress:

* Use singular object names for fact and dimension tables.
* Prefix tables with `dim_` or `fact_`.
* Preface system-specific IDs with abbreviated prefix, such as `track_id` to `app_track_id`.
* Dimensions should not include any null values.
* Dimensions should be common (conformed) across all fact tables.
* End-users will typically be working with joined dimension + fact tables, so dimension field names should be qualified with the name of the dimension, e.g. `member_geo` instead of `geo`.
