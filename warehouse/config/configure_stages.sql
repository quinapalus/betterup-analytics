-- Run these rols as tap_loader to simplify the ownership configuration
use role tap_loader;

-- Use the access key/secret for an IAM user with the
-- ReadOnlyWarehouseTapsS3 policy attached.
create or replace stage raw.tap_mindtickle.base
url='s3://bu-warehouse-taps'
encryption=(type='AWS_SSE_S3')
credentials=(AWS_KEY_ID='...' AWS_SECRET_KEY='...');
create file format if not exists raw.tap_mindtickle.csv type = 'CSV' escape='\\' field_optionally_enclosed_by='"';


create or replace stage raw.fountain.base
url='s3://bu-warehouse-taps'
encryption=(type='AWS_SSE_S3')
credentials=(AWS_KEY_ID='...' AWS_SECRET_KEY='...');
create file format if not exists raw.tap_fountain.csv type = 'CSV' escape='\\' field_optionally_enclosed_by='"';

use role ACCOUNTADMIN;
CREATE OR REPLACE STORAGE INTEGRATION call_events
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::270286309069:role/snowflake_call_events_s3'
    STORAGE_ALLOWED_LOCATIONS = ('s3://betterup-video-us-east-1-staging/', 's3://betterup-video-us-east-1-production/');

use role TRANSFORMER;
create or replace stage raw.p2pc.call_events_staging_stage
    URL = 's3://betterup-video-us-east-1-staging'
    STORAGE_INTEGRATION = call_events
    FILE_FORMAT = (TYPE = 'JSON')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

create or replace stage raw.p2pc.call_events_production_stage
    URL = 's3://betterup-video-us-east-1-production'
    STORAGE_INTEGRATION = call_events
    FILE_FORMAT = (TYPE = 'JSON')
    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
