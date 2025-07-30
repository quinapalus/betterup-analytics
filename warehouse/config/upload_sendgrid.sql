use database raw;
use role stitch_loader;
drop schema stitch_sendgrid;
create schema stitch_sendgrid;
grant all on schema stitch_sendgrid to role stitch_loader;

create or replace file format raw.stitch_sendgrid.csv type='CSV' field_delimiter=';' field_optionally_enclosed_by='"' compression='gzip';
create or replace stage raw.stitch_sendgrid.base
url='s3://bu-warehouse-taps/sendgrid'
encryption=(type='AWS_SSE_S3')
credentials=(AWS_KEY_ID='...' AWS_SECRET_KEY='...')
file_format='raw.stitch_sendgrid.csv';

grant ownership on stage raw.stitch_sendgrid.base to role stitch_loader;
grant ownership on file format raw.stitch_sendgrid.csv to role stitch_loader;

create table raw.stitch_sendgrid.events as
select
("$1") "_sdc_batched_at" , ("$2") "_sdc_received_at" , ("$3") "_sdc_sequence" , ("$4") "_sdc_table_version" , ("$5") "asm_group_id" , ("$6") "attempt" , ("$7") "category" , ("$8") "email" , ("$9") "event" , ("$10") "ip" , ("$11") "reason" , ("$12") "response" , ("$13") "sg_event_id" , ("$14") "sg_message_id" , ("$15") "smtp-id" , ("$16") "status" , ("$17") "timestamp" , ("$18") "url" , ("$19") "useragent" , ("$20") "tls" , ("$21") "url_offset__index" , ("$22") "url_offset__type" , ("$23") "cert_err" , ("$24") "type" , ("$25") "date" , ("$26") "processed" , ("$27") "send_at" , ("$28") "sg_content_type" , ("$29") "user_id"
from @raw.stitch_sendgrid.base/stitch_sendgrid_events.csv.gz;

drop table raw.stitch_sendgrid.events__category;
create table raw.stitch_sendgrid.events__category as
select
("$1") "_sdc_batched_at" , ("$2") "_sdc_level_0_id" , ("$3") "_sdc_received_at" , ("$4") "_sdc_sequence" , ("$5") "_sdc_source_key_email" , ("$6") "_sdc_source_key_event" , ("$7") "_sdc_source_key_timestamp" , ("$8") "_sdc_table_version" , ("$9") "value"
from @raw.stitch_sendgrid.base/stitch_sendgrid_events_category.csv.gz;

create table raw.stitch_sendgrid.events__resource_ids as
select
("$1") "_sdc_batched_at" , ("$2") "_sdc_level_0_id" , ("$3") "_sdc_received_at" , ("$4") "_sdc_sequence" , ("$5") "_sdc_source_key_email" , ("$6") "_sdc_source_key_event" , ("$7") "_sdc_source_key_timestamp" , ("$8") "_sdc_table_version" , ("$9") "value"
from @raw.stitch_sendgrid.base/stitch_sendgrid_events_resource_ids.csv.gz;

