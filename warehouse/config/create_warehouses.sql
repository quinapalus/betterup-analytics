-- must run as `account_admin`
create database raw;
create database analytics;
create database staging_performance;

-- Create database and schemas for https://github.com/betterup/betterup_data_action_server
create database data_action_server;
create schema data_action_server.production;
create schema data_action_server.test;
create schema data_action_server.dev;


create warehouse stitch_loading
    warehouse_size = xsmall
    auto_suspend = 3600
    auto_resume = false
    initially_suspended = true;

create warehouse tap_loading
    warehouse_size = xsmall
    auto_suspend = 3600
    auto_resume = false
    initially_suspended = true;

create warehouse segment_loading
    warehouse_size = xsmall
    auto_suspend = 3600
    auto_resume = false
    initially_suspended = true;

create warehouse transforming
    warehouse_size = xsmall
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true;

create warehouse reporting
    warehouse_size = xsmall
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true;

create warehouse app_reporting
    warehouse_size = xsmall
    auto_suspend = 300
    auto_resume = true
    initially_suspended = true;

create warehouse identify_loading
    warehouse_size = xsmall
    auto_suspend = 300
    auto_resume = true
    initially_suspended = true;

create warehouse monte_carlo_wh
    warehouse_size = xsmall
    auto_suspend = 5
    auto_resume = true
    initially_suspended = true;

create warehouse p2pc_wh
    warehouse_size = xsmall
    auto_suspend = 5
    auto_resume = true
    initially_suspended = true;