# script to process values from https://github.com/betterup/betterup-app/blob/master/config/assessment_definitions/whole_person/2.0/benchmarks.yml into csv
#
# usage:
#   $ python parse_benchmarks.py > bu_whole_person_benchmarks.csv
#

import os
import yaml
import re

roles = ('Individual Contributor', 'Manager')

# create a dict with any subdimension_keys we need to rename from benchmarks.yml
construct_key_dict = {
  'locus_of_control' : 'locus_of_control_internal',
  'employee_experience' : 'ex_index'
  }

# create regex pattern using roles tuple:
re_industry_role = re.compile('^(.+) (' + '|'.join(roles) + ')$')

# TODO RF: update this script to pull file from betterup-app github repo directly
# near term hack: download https://github.com/betterup/betterup-app/blob/master/config/assessment_definitions/whole_person/2.0/benchmarks.yml into Downloads folder
with open(os.path.expanduser("~/Downloads/benchmarks.yml")) as file:
    # load yml data into dict
    benchmarks = yaml.load(file)

# output csv column headers
print("whole_person_model_version,industry,employee_level,construct_key,scale_score_mean")

# iterate over each item in dict
for industry_role,subdimensions in benchmarks.items():
    # parse industry_role into separate industry and role fields:
    if industry_role in roles:
        # role only
        industry = ''
        role = industry_role
    elif industry_role.endswith(roles):
        # industry-role pair
        m = re_industry_role.match(industry_role)
        industry = m.groups()[0]
        role = m.groups()[1]
    else:
        # industry only
        industry = industry_role
        role = ''

    for subdimension_key,scale_score_mean in subdimensions.items():
        # update any subdimension_keys we need to alias
        construct_key = subdimension_key if construct_key_dict.get(subdimension_key) is None else construct_key_dict.get(subdimension_key)

        print("{},{},{},{},{}".format('WPM 2.0', industry, role, construct_key, scale_score_mean))
