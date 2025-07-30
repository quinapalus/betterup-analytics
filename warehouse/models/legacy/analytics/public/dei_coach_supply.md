{% docs dei_coach_supply %}

This model processes coach supply and staffing attributes into row based format. Each row represents a supply category (`attribute_scope`), `attribute_value`, `attribute_label`, and associated `coach_id`.

Example query to select all coaches that available to work with director-level members:
```
  SELECT
    coach_id
  FROM analytics.dei_coach_supply
  WHERE attribute_scope = 'member_level'
    AND attribute_value = 'director'
```
{% enddocs %}

{% docs dei_coach_supply__attribute_scope %}

The values in the `attribute_scope` field are high-level categories of coach supply, relevant for staffing coaches and managing the coach network.

| attribute_scope | description | attribute_value |
|-----------------|-------------|-----------------|
| geo             | Geographic region. | `NORAM` - Northern America, `LATAM` - Latin America, `APAC` - Asia Pacific, `EMEA` - Europe, Middle East, Africa |
| country         | Country based on a coach's time zone and currency. | 2 letter ISO-3166 country code. See associated `attribute_label` for human-friendly country name. |
| tier            | BetterUp coaching level | `associate`, `fellow` |
| member_level    | Organizational level that a coach is eligible to work with. | `individual_contributor`, `frontline_manager`, `director`, `vp/svp`, `c_level` |
| language        | Language that the coach can coach in. | ISO 639-1 Alpha-2 code, e.g. `en`, `es`, `zh`. See `attribute_label` for human-friendly language name. |
| certification   | External certifications. | e.g. `icf_pcc`, `icf_acc`, `cpc` |
| industry        | Industries where the coach is experienced. | e.g. `healthcare`, `military`, `sales`, `government`, `manufacturing` |

{% enddocs %}
