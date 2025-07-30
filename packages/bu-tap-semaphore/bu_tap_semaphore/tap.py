"""
Singer tap for pulling data from Semaphore

Currently supports:
    * Pulling Semaphore CI build information
"""
import json
import time

import arrow
import singer
from singer import utils, Transformer
import requests
from requests.adapters import HTTPAdapter
from requests.adapters import Retry

from . import schema


REQUIRED_CONFIG_KEYS = ["semaphore_token", "semaphore_hours"]
LOGGER = singer.get_logger()

REQUEST_URL = "https://betterup.semaphoreci.com/api/v1alpha/jobs?states=3&order=0"


MAX_PAGES = 10000
"""Max number of pages to retrieve (prevents an infinite loop)"""

MAX_ATTEMPTS = 100
"""Maximum number of retries on failing requests."""


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        sync(args.config, args.state)


def discover():
    raw_schemas = {"jobs": schema.jobs}
    streams = []

    for schema_name, schema_details in raw_schemas.items():
        # create and add catalog entry
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema_details,
            "metadata": {"selected": True},
            "key_properties": [],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def create_requests_session(headers):
    session = requests.Session()
    retry = Retry(total=MAX_ATTEMPTS, read=3, connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(headers)

    return session


def sync(config, state):
    LOGGER.info("Writing job information")
    write_jobs(config)


def write_jobs(config):
    api_key = config["semaphore_token"]
    hours_to_sync = int(config["semaphore_hours"])
    singer.write_schema("job", schema.jobs, ["id"])

    # Will run on an hourly schedule so capture last three hours of builds to
    # account for any missing runs.
    jobs_after = arrow.utcnow().shift(hours=-hours_to_sync)
    for job in list_jobs(api_key, jobs_after):
        envvars = {
            envvar["name"]: envvar["value"]
            for envvar in job["spec"]["env_vars"]
            if "value" in envvar and "name" in envvar
        }
        LOGGER.info("%s", f'Found job: {job["metadata"]["id"]}')

        create_time = job["metadata"].get("create_time")
        if create_time is not None:
            create_time = arrow.get(int(create_time)).isoformat()

        update_time = job["metadata"].get("update_time")
        if update_time is not None:
            update_time = arrow.get(int(update_time)).isoformat()

        start_time = job["metadata"].get("start_time")
        if start_time is not None:
            start_time = arrow.get(int(start_time)).isoformat()

        finish_time = job["metadata"].get("finish_time")
        if finish_time is not None:
            finish_time = arrow.get(int(finish_time)).isoformat()

        info = {
            "id": job["metadata"]["id"],
            "create_time": create_time,
            "update_time": update_time,
            "start_time": start_time,
            "finish_time": finish_time,
            "name": job["metadata"]["name"],
            "project_id": job["spec"]["project_id"],
            "agent_type": job["spec"]["agent"]["machine"]["type"],
            "os_image": job["spec"]["agent"]["machine"].get("os_image"),
            "branch": envvars.get("SEMAPHORE_GIT_BRANCH"),
            "git_sha": envvars.get("SEMAPHORE_GIT_SHA"),
            "git_repo": envvars.get("SEMAPHORE_GIT_REPO_SLUG"),
            "workflow_id": envvars.get("SEMAPHORE_WORKFLOW_ID"),
            "result": job["status"]["result"],
        }

        with Transformer() as transformer:
            record = transformer.transform(info, schema.jobs)

        singer.write_record("job", record)


def list_jobs(api_key, after):
    current = None
    i = 0
    session = create_requests_session({"Authorization": f"Token {api_key}"})
    while i <= MAX_PAGES:
        params = {} if current is None else {"page_token": current}
        for i in range(MAX_ATTEMPTS):
            response = session.get(REQUEST_URL, params=params)
            if response.status_code == 200:
                break
            time.sleep(0.1 * i)
        else:
            response.raise_for_status()

        data = response.json()

        current = data["next_page_token"]
        for job in data["jobs"]:
            yield job

            if arrow.get(int(job["metadata"]["create_time"])) < after:
                return StopIteration


if __name__ == "__main__":
    main()
