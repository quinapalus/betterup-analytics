{% docs deployment_types %}

In general, deployment types specify what kind of product service is offered to users of the BetterUp platform, in addition to specifying who the end users are.

| deployment_type | description | accounting_category | is_revenue_generating | is_external |
|-----------------|-------------|---------------------|-----------------------|-------------|
| `marketing_existing_client` | Partner or key stakeholders of an organization (existing booked business) are offered coaching as a relationship-building and up-selling tool. | 7551 MKTG Coaching Promotional | false | true |
| `marketing_prospective_client` | Coaching offered to potential business down the line, primarily lead by client partnership team. Intent is to build strong relationships for future leads, but members in this cohort are not part of the standard sales process in which a sales trial would be a stage in the sales cycle. Not connected to an AE or SF Opportunity. | 7551 MKTG Coaching Promotional | false | true |
| `marketing_other` | Coaching offered to potential future investors or partners, requests primarily lead by the founders. Intent is to build strong relationships for leads and investments. These are individuals, rather than organizations and are outside of the sales cycle. Not associated with an AE or SF Opportunity. | 7551 MKTG Coaching Promotional | false | true |
| `bu_employee` | Coaching as an employee benefit, for BetterUp employees. | 6492 BU-Employee Coaching | false | false |
| `bu_friends_and_family` | Coaching as an employee benefit, for BetterUp employees' significant others. | 6494 BU-F&F Coaching | false | false |
| `candidates` | Three-sessions test drive for potential employees, as part of the interview process. | N/A | false | false |
| `research_and_development` | Internal research and development programs. | 7120 R&D Other Expenses | false | true |
| `standard` | Enterprise accounts. | 5311 Customer Sessions | true | true |
| `standard_smb` | Small and medium-sized accounts. | 5311 Customer Sessions | true | true |
| `trial` | Trial for potential business within enterprise accounts. Offers members a chance to test the coaching experience. Part of the sales process.  | 7550 Sales Trials | false | true |
| `trial_smb` | Trial for potential business within small and medium-sized accounts. Offers members a chance to test the coaching experience. Part of the sales process. | 7550 Sales Trials | false | true |
| `pilot` | Smaller deployment ahead of larger program launch within enterprise accounts. | 5311 Customer Sessions | true | true |
| `pilot_smb` | Smaller deployment ahead of larger program launch within small and medium-sized accounts. | 5311 Customer Sessions | true | true |
| `qa` | Quality-assurance or demo account. | N/A | false | false |
| `private_pay` | Members who opt into coaching outside of a program sponsored by an organization with existing booked business. | 5311 Customer Sessions | true | true |
| `galen` | Deployments that are part of Operation Galen initiated in March 2020, which encompasses both pay-as-you-go billing model and free coaching to particular groups, such as teachers and healthcare workers. Term Galen is used internally within BetterUp, while BetterUp4Community is used externally with partners. | 7551 MKTG Coaching Promotional | true | true |

{% enddocs %}
