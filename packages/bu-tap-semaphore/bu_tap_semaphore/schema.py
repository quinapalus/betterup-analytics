"""
Contains the Singer schemas as dictionaries
for consumption in the tap.



Create job schema
Job ID (metadata --> id)
Job Name (metadata --> name)
Project ID (spec --> project_id)
Create Time (metadata --> create_time)
Update Time (metadata --> update_time)
Start Time (metadata --> start_time)
Finish Time (metadata --> finish_time)
Machine type (spec --> agent --> type)
OS Image (spec --> agent --> os_image)
Branch (spec --> env_vars --> value where name = SEMAPHORE_GIT_BRANCH)
Git SHA (spec --> env_vars --> value where name = SEMAPHORE_GIT_SHA)
Git Repo (spec --> env_vars --> value where name = SEMAPHORE_GIT_REPO_SLUG)
Workflow ID (spec --> env_vars --> value where name = SEMAPHORE_WORKFLOW_ID)
Result (status --> result)
"""


job_string_fields = [
    "id",
    "name",
    "project_id",
    "agent_type",
    "os_image",
    "branch",
    "git_sha",
    "git_repo",
    "workflow_id",
    "result",
]

job_date_fields = [
    "create_time",
    "update_time",
    "start_time",
    "finish_time",
]


job_boolean_fields = []


job_array_fields = []


job_properties = {}
for field in job_string_fields:
    job_properties[field] = {"type": ["null", "string"]}

for field in job_date_fields:
    job_properties[field] = {"type": ["null", "string"], "format": "date-time"}

for field in job_boolean_fields:
    job_properties[field] = {"type": ["boolean"]}

for field in job_array_fields:
    job_properties[field] = {"type": ["null", "array"], "items": {"type": "string"}}


jobs = {
    "type": ["object"],
    "additionalProperties": False,
    "properties": job_properties,
}
