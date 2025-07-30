import os
import fileinput
import sys
import json
import pandas as pd
import click
import click_log
import logging
from datetime import datetime
from json_to_csv import JSONtoCSV
from file_mapping import FileMapping

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


def open_output_file(outputPath):
    outputFilename = outputPath + "json_csv_output_" + str(datetime.now()) + ".csv"
    try:
        outputFile = open(outputFilename, "w")
    except:
        logger.error("Could not open file: " + outputFilename)
        sys.exit(1)

    return outputFile


@click.command()
@click.option(
    "--root",
    default=None,
    help="Root directory containing sovren output and Mode extracts",
)
@click.option("--output", default=None, help="Output directory to store csv output")
@click_log.simple_verbosity_option(logger)
def main(root, output):

    outputFile = open_output_file(output)

    json_parser = JSONtoCSV()

    file_mapping_parser = FileMapping()

    rowsList = []

    fileMappings = []

    sovrenPath = root + "sovren output/"
    # Read files from each resume source
    for resumeSource in os.listdir(sovrenPath):
        if not resumeSource.startswith("."):
            # read json files and write to csv
            logger.info("Reading resumes from " + resumeSource + "...")
            jsonPath = sovrenPath + resumeSource + "/json/"
            for fileName in os.listdir(jsonPath):
                with open(jsonPath + fileName, "r") as f:
                    resume = json.load(f)
                    json_parser.extract_attributes(resume, resumeSource, fileName)
                    rowsList.append(json_parser.get_clean_row())

            # read file mapping logs and store in fileMappings
            logger.info("Extracting file mappings from logs...")
            logPath = sovrenPath + resumeSource + "/logs/"
            for logName in os.listdir(logPath):
                if logName.endswith("file_mappings.log"):
                    with open(logPath + logName) as f:
                        newMappings = file_mapping_parser.parse(f, resumeSource)
                        fileMappings = fileMappings + file_mapping_parser.get_mappings()

    logger.info("Composing DataFrames...")
    # Create data frame from rows list
    resume_coaches = pd.DataFrame(rowsList, columns=json_parser.get_headers())
    firstnames = resume_coaches["FirstName"].str.lower().str.replace("[^a-zA-Z]", "")
    lastnames = resume_coaches["LastName"].str.lower().str.replace("[^a-zA-Z]", "")
    resume_coaches["full_name"] = firstnames + lastnames

    # Create data frame from mappings list
    file_mappings = pd.DataFrame(
        fileMappings, columns=file_mapping_parser.get_headers()
    )

    logger.info("Importing mode extracts...")
    # Extract CSV data from Mode into DataFrames
    modePath = root + "mode extracts/"
    dei_coaches = pd.read_csv(modePath + "dei coaches extract.csv")
    firstnames = dei_coaches["first_name"].str.lower().str.replace("[^a-zA-Z]", "")
    lastnames = dei_coaches["last_name"].str.lower().str.replace("[^a-zA-Z]", "")
    dei_coaches["full_name"] = firstnames + lastnames

    coach_leaderboard = pd.read_csv(modePath + "coach leaderboard extract.csv")
    cleannames = (
        coach_leaderboard["coach_name"].str.lower().str.replace("[^a-zA-Z]", "")
    )
    coach_leaderboard["full_name"] = cleannames

    # add admin panel urls to dei_coaches
    admin_urls = "https://app.betterup.co/admin/users/" + dei_coaches[
        "coach_id"
    ].astype(str)
    dei_coaches["admin_url"] = admin_urls

    # add orginal file names to resume_coaches
    resume_coaches = resume_coaches.merge(
        file_mappings, how="left", left_on="FileName", right_on="json"
    )

    # join dei_coaches with coach_leaderboard
    mode_data = dei_coaches.merge(coach_leaderboard, how="left", on="full_name")

    # join mode_data on to resume_coaches
    final_data = resume_coaches.merge(mode_data, how="left", on="full_name")

    # re-order columns
    final_cols = [
        "ResumeSource",
        "OriginalFile",
        "FormattedName",
        "FirstName",
        "LastName",
        "Municipality",
        "CountryCode",
        "YearsOfExperience",
        "ExperienceSummary",
        "NumberOfEmployers",
        "EmployerNames",
        "EducationDetail",
        "Certifications",
        "Languages",
        "coach_id",
        "admin_url",
    ] + coach_leaderboard.columns.tolist()
    final_data = final_data[final_cols]

    final_data.to_csv(outputFile)

    outputFile.close()

    sys.exit(0)


if __name__ == "__main__":
    main()
