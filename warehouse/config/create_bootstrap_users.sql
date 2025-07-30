USE ROLE ACCOUNTADMIN;

CREATE ROLE terraform_role;
GRANT ROLE SYSADMIN TO ROLE terraform_role;
GRANT ROLE SECURITYADMIN TO ROLE terraform_role;
GRANT ROLE USERADMIN TO ROLE terraform_role;
GRANT ROLE terraform_role TO ROLE ACCOUNTADMIN;
GRANT CREATE INTEGRATION ON ACCOUNT TO ROLE terraform_role;
CREATE USER "tf-snow" PASSWORD = '********' DEFAULT_ROLE='TERRAFORM_ROLE' MUST_CHANGE_PASSWORD=FALSE;
GRANT ROLE terraform_role TO USER "tf-snow";


CREATE ROLE IF NOT EXISTS okta_provisioner;
GRANT CREATE USER ON ACCOUNT TO ROLE okta_provisioner;
GRANT CREATE ROLE ON ACCOUNT TO ROLE  okta_provisioner;
GRANT ROLE okta_provisioner TO ROLE ACCOUNTADMIN;
GRANT ROLE okta_provisioner TO ROLE terraform_role;


-- Grants to be run after Okta integrations are created
GRANT OWNERSHIP ON INTEGRATION okta_provisioning TO ROLE terraform_role;
GRANT OWNERSHIP ON INTEGRATION okta_saml_integration TO ROLE terraform_role;
