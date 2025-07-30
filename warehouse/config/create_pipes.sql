create or replace TABLE RAW.P2PC.CALL_EVENTS (
    UUID varchar(36) default uuid_string() not null,
   	CALL_ID NUMBER(38,0) not null,
   	CONTACT_METHOD VARCHAR(16777216),
   	CREATED_AT TIMESTAMP_TZ(9),
   	DATA VARCHAR(16777216),
   	EVENT_NAME VARCHAR(16777216),
   	UPDATED_AT TIMESTAMP_TZ(9),
   	USER_ID NUMBER(38,0) not null,
    SOURCE varchar(20);

create or replace pipe p2pc.ingest_production_events auto_ingest=false as
    copy into RAW.P2PC.CALL_EVENTS
        from @RAW.P2PC.CALL_EVENTS_PRODUCTION_STAGE
        file_format = (type = 'JSON')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;