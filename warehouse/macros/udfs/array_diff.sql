{% macro array_diff() %}
CREATE OR REPLACE FUNCTION {{ target.schema }}.ARRAY_DIFF(ARRAY1 VARIANT, ARRAY2 VARIANT)
    returns VARIANT
    language JAVASCRIPT
    comment = 'Returns elements in the first array that are not contained in the second array.'
    as $$
        var difference = ARRAY1.filter(x => !ARRAY2.includes(x));
        return Array.from(new Set(difference)).sort();
    $$
;
{% endmacro %}