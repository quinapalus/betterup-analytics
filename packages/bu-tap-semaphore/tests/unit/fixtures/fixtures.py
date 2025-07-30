def get_mock_env_vars():
    return {
        "SNOWFLAKE_ACCOUNT": "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER": "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD": "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DB": "SNOWFLAKE_DB",
        "SNOWFLAKE_WAREHOUSE": "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_SEMAPHORE_SCHEMA": "SNOWFLAKE_SEMAPHORE_SCHEMA",
        "TAP_SEMAPHORE_STAGE": "TAP_SEMAPHORE_STAGE",
        "TAP_SEMAPHORE_FILE_FORMAT": "TAP_SEMAPHORE_FILE_FORMAT",
        "TAP_S3_ACCESS_KEY": "TAP_S3_ACCESS_KEY",
        "TAP_S3_SECRET_KEY": "TAP_S3_SECRET_KEY",
        "TAP_SEMAPHORE_S3_KEY_PREFIX": "TAP_SEMAPHORE_S3_KEY_PREFIX",
        "TAP_S3_BUCKET": "TAP_S3_BUCKET",
        "SNOWFLAKE_TRANSFORMER_ROLE": "SNOWFLAKE_TRANSFORMER_ROLE",
        "SEMAPHORE_TOKEN": "SEMAPHORE_TOKEN",
        "SEMAPHORE_HOURS": "SEMAPHORE_HOURS"
    }

def get_semaphore_config():
    return {"semaphore_token": "test", "semaphore_hours": "3"}

def get_time():
    return 1674239307

def get_semaphore_response():
    return {
        "jobs": [
            {
                "metadata": {
                    "name": "System - 31/36",
                    "id": "fdd80b17-4237-4bb3-ada6-5309871454a3",
                    "create_time": "1674239307",
                    "update_time": "1674239629",
                    "start_time": "1674239313",
                    "finish_time": "1674239629",
                },
                "spec": {
                    "project_id": "82987496-3bdc-4dcb-9571-fc0e7d04270a",
                    "agent": {
                        "machine": {"type": "f1-standard-2", "os_image": "ubuntu2004"}
                    },
                    "env_vars": [
                        {
                            "name": "SEMAPHORE_WORKFLOW_ID",
                            "value": "faefde71-4ca4-496f-a252-1f75678a71a9",
                        },
                        {
                            "name": "SEMAPHORE_GIT_SHA",
                            "value": "40005524cf530e06d43758a40c1a97b9d987f5ad",
                        },
                        {
                            "name": "SEMAPHORE_GIT_REPO_SLUG",
                            "value": "betterup/betterup-app",
                        },
                        {"name": "SEMAPHORE_GIT_BRANCH", "value": "main"},
                    ],
                },
                "status": {
                    "result": "PASSED",
                    "state": "FINISHED",
                },
            },
            {
                "metadata": {
                    "name": "Assets",
                    "id": "fd25605e-7fb3-4371-8cf9-2c49j87aa2d3",
                    "create_time": "1674239307",
                    "update_time": "1674239565",
                    "start_time": "1674239314",
                    "finish_time": "1674239565",
                },
                "spec": {
                    "project_id": "82093296-3bdc-4dcb-9571-fc0e7d04270a",
                    "agent": {
                        "machine": {"type": "f1-standard-2", "os_image": "ubuntu2004"}
                    },
                    "secrets": [{"name": "docker-auth"}],
                    "env_vars": [
                        {
                            "name": "SEMAPHORE_WORKFLOW_ID",
                            "value": "faefde71-4ca4-496f-a252-1f09878a71a9",
                        },
                        {
                            "name": "SEMAPHORE_GIT_SHA",
                            "value": "48765524cf530e06d43758a40c1a97b9c857f5ad",
                        },
                        {
                            "name": "SEMAPHORE_GIT_REPO_SLUG",
                            "value": "betterup/betterup-app",
                        },
                        {"name": "SEMAPHORE_GIT_BRANCH", "value": "main"},
                    ],
                },
                "status": {
                    "result": "PASSED",
                    "state": "FINISHED",
                },
            },
            {
                "metadata": {
                    "name": "Frontend - 4/10",
                    "id": "f976c054-b907-4778-ae2f-bc13be3e4f7f",
                    "create_time": "1674228506",
                    "update_time": "1674239700",
                    "start_time": "1674239313",
                    "finish_time": "1674239700",
                },
                "spec": {
                    "project_id": "82865486-3bdc-4dcb-9571-fc0e7d04270a",
                    "agent": {
                        "machine": {"type": "e1-standard-2", "os_image": "ubuntu2004"}
                    },
                    "secrets": [
                        {"name": "docker-auth"},
                        {"name": "code-climate"},
                        {"name": "flaky-test-reporting"},
                    ],
                    "env_vars": [
                        {
                            "name": "SEMAPHORE_WORKFLOW_ID",
                            "value": "faefde71-4ca4-496f-a252-1f79807a71a9",
                        },
                        {
                            "name": "SEMAPHORE_GIT_SHA",
                            "value": "40000987cf530e06d43758a40c1a97b9c857f5ad",
                        },
                        {
                            "name": "SEMAPHORE_GIT_REPO_SLUG",
                            "value": "betterup/betterup-app",
                        },
                        {"name": "SEMAPHORE_GIT_BRANCH", "value": "main"},
                    ],
                },
                "status": {
                    "result": "PASSED",
                    "state": "FINISHED",
                },
            },
        ],
        "next_page_token": "g3QAAAACZAAKIJBBRlZF9hdHQ9zdHJ1Y3RfX2QAD0VsaXhpci5EYXRlVGltZWQACGNhbGVuZGFyZAATRWxpeGlyLkNhbGVuZGFyLklTT2QAA2RheWEXZAAEaG91cmEQZAALbWljcm9zZWNvbmRoAmEAYQBkAAZtaW51dGVhKmQABW1vbnRoYQFkAAZzZWNvbmRhFWQACnN0ZF9vZmZzZXRhAGQACXRpbWVfem9uZW0AAAAHRXRjL1VUQ2QACnV0Y19vZmZzZXRhAGQABHllYXJiAAAH52QACXpvbmVfYWJicm0AAAADVVRDZAACaWRtAAAAJGM4NDI5MWE3LTZjMjYtNGJiOS1iMjc3LTBiYTYxYzhkZjRkOA==",
    }

def get_discover(): 
    return {
        "streams": [
            {
            "stream": "jobs",
            "tap_stream_id": "jobs",
            "schema": {
                "type": [
                "object"
                ],
                "additionalProperties": False,
                "properties": {
                "id": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "name": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "project_id": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "agent_type": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "os_image": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "branch": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "git_sha": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "git_repo": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "workflow_id": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "result": {
                    "type": [
                    "string",
                    "null"
                    ]
                },
                "create_time": {
                    "type": [
                    "string",
                    "null"
                    ],
                    "format": "date-time"
                },
                "update_time": {
                    "type": [
                    "string",
                    "null"
                    ],
                    "format": "date-time"
                },
                "start_time": {
                    "type": [
                    "string",
                    "null"
                    ],
                    "format": "date-time"
                },
                "finish_time": {
                    "type": [
                    "string",
                    "null"
                    ],
                    "format": "date-time"
                }
                }
            },
            "metadata": {
                "selected": True
            },
            "key_properties": []
            }
        ]
    }
