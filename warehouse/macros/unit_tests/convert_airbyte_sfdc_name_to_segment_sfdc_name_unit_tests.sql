{% macro airbyte_sfdc_AccountName_assertion() %}

{%- set column_name = convert_airbyte_sfdc_name_to_segment_sfdc_name('AccountName') -%}

{{ dbt_unittest.assert_equals(column_name, 'account_name') }}

{% endmacro %}

{% macro airbyte_sfdc_Account_Name__c_assertion() %}

{%- set column_name = convert_airbyte_sfdc_name_to_segment_sfdc_name('Account_Name__c') -%}

{{ dbt_unittest.assert_equals(column_name, 'account_name_c') }}

{% endmacro %}

{% macro airbyte_sfdc_X2022_Global_2000__c_assertion() %}

{%- set column_name = convert_airbyte_sfdc_name_to_segment_sfdc_name('X2022_Global_2000__c') -%}

{{ dbt_unittest.assert_equals(column_name, 'x2022_global_2000_c') }}

{% endmacro %}

{% macro airbyte_sfdc_X1st_Session_Completed__c_assertion() %}

{%- set column_name = convert_airbyte_sfdc_name_to_segment_sfdc_name('X1st_Session_Completed__c') -%}

{{ dbt_unittest.assert_equals(column_name, 'x1_st_session_completed_c') }}

{% endmacro %}

{% macro airbyte_sfdc_Outcomes_BU_Measured__c_assertion() %}

{%- set column_name = convert_airbyte_sfdc_name_to_segment_sfdc_name('Outcomes_BU_Measured__c') -%}

{{ dbt_unittest.assert_equals(column_name, 'outcomes_bu_measured_c') }}

{% endmacro %}

{% macro airbyte_sfdc_Participate_in_the_BU_community__c_assertion() %}

{%- set column_name = convert_airbyte_sfdc_name_to_segment_sfdc_name('Participate_in_the_BU_community__c') -%}

{{ dbt_unittest.assert_equals(column_name, 'participate_in_the_bu_community_c') }}

{% endmacro %}

{% macro airbyte_sfdc_Product2Id_assertion() %}

{%- set column_name = convert_airbyte_sfdc_name_to_segment_sfdc_name('Product2Id') -%}

{{ dbt_unittest.assert_equals(column_name, 'product2_id') }}

{% endmacro %}

{% macro airbyte_mkto71_Lead_Score__c_assertion() %}

{%- set column_name = convert_airbyte_sfdc_name_to_segment_sfdc_name('mkto71_Lead_Score__c') -%}

{{ dbt_unittest.assert_equals(column_name, 'mkto_71_lead_score_c') }}

{% endmacro %}

{% macro airbyte_Whole_Person_360_Enabled__c_assertion() %}

{%- set column_name = convert_airbyte_sfdc_name_to_segment_sfdc_name('whole_person_360_enabled_c') -%}

{{ dbt_unittest.assert_equals(column_name, 'whole_person_360_enabled_c') }}

{% endmacro %}
