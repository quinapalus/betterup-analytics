# bu-sfdc-integrations

# Description

`bu-sfdc-integrations` contains various tools and scripts for
interacting with Salesforce.

Tools here are meant to help augment Salesforce data from production data to enable
our Sales teams. As such, data acceptable to synchronize to Salesforce is maintained in the
`salesforce_service` schema in the warehouse (Snowflake). If you have questions over what data
is available or would like to extend that, check out the [associated models](../../warehouse/models/services/salesforce).


*Included tools*
* (update-sfdc-contacts)[#update-sfdc-contacts]: Update Salesforce contact BetterUp platform status.


# update-sfdc-contacts

`update-sfdc-contacts` is responsible for updating Salesforce Contacts with information from
their usage on BetterUp. When run, each salesforce contact that is on the BetterUp platform is updated
with the following fields:
- `Current_User__c`
- `Past_User__c`
- `Program_Admin__c`

Adding these flags to Salesforce contacts enables the post-sales team to customize future expansion sales outreach based on the contact's previous or current status as a BetterUp member or deployment admin.

Example:
```
update-sfdc-contacts
```

# Installation

For developers, you can install an editable version of the project by running

```
pip install -e .
```

If you want a production version then install

```
pip install .
```


