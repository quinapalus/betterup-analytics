# Overview
This README provides high level documentation on the workflow jobs housed in this repository. 

## Code organization
We currently have nine workflow jobs being run off of Github Actions, to be migrated to another workflow orchestration platform. These are:
- fountain-snowflake-sync
- looker-s3-sync
- qualtrics-s3-sync
- semaphore-s3-sync
- semaphore-snowflake-sync
- twilio-s3-sync
- update-sfdc-accounts
- update-sfdc-contacts
- zendesk-s3-sync

All the YAML files driving the work are located under the `.github/workflows` folder. These YAML files run a shell script, which can be found in the `bin` folder. The core logic of each workflow job (written in python) can be found under the respective subfolder in the `packages` folder. The shell scripts will `cd` into the respective subfolder, install all dependencies using poetry package manager, create bash commands that link to python functions, and run the respective bash commands.

## Debugging/past run logs

Logs from previous runs of each workflow can be found in the [actions tab](https://github.com/betterup/betterup-analytics/actions) of this github repo. Simply find the name of the workflow, and click on any of the runs to see detailed logs.

![image](https://user-images.githubusercontent.com/122490701/223837878-dc62615c-b5e9-4b7d-bc67-5223d0cc959e.png)

![image](https://user-images.githubusercontent.com/122490701/223838339-bd356ef2-07b8-4b91-bc6c-1abf7ae0bd1c.png)
