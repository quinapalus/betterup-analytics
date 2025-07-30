{% macro encode_coach_creation_url() %}

CREATE OR REPLACE FUNCTION {{ target.schema }}.encode_coach_creation_url(first_name string,
                                                                         last_name string,
                                                                         time_zone string,
                                                                         email string,
                                                                         phone string,
                                                                         title string,
                                                                         organization_id string,
                                                                         tier string,
                                                                         primary string,
                                                                         coaching_language string,
                                                                         roles array,
                                                                         fountain_applicant_id string,
                                                                         languages array,
                                                                         member_levels array,
                                                                         industries array,
                                                                         risk_level string,
                                                                         coaching_certifications array,
                                                                         non_icf_cert string,
                                                                         certification_mbti string,
                                                                         focus_areas array,
                                                                         products array)
RETURNS string
    LANGUAGE python
    runtime_version = 3.8
    handler = 'encode_url'
    comment = 'Encodes the url query params necessary for coach creation in admin panel'
    as
$$
import urllib.parse

def cert_map(cert_list):
  if not cert_list:
    return []

  tag_map = {'ICF - ACC' : 'ICF_ACC',
             'ICF - PCC' : 'ICF_PCC',
             'ICF - MCC' : 'ICF_MCC',
             'CCE - BCC - Licensed' : 'BCC',
             'BCC' : 'BCC',
             'CEC' : 'CEC',
             'CPC' : 'CPC',
             'CPCC' : 'CPCC',
             'EMCC' : 'EMCC',
             'Non ICF/CCE' : 'non_ICF',
             'Cert in process' : 'in_progress',
             'MBTI' : 'mbti'
             }
  # filter non-matching elements out of list
  return list(filter(None, map(tag_map.get, cert_list)))

def encode_url(first_name,
    last_name,
    time_zone,
    email,
    phone,
    title,
    organization_id,
    tier,
    primary,
    coaching_language,
    roles,
    fountain_applicant_id,
    languages,
    member_levels,
    industries,
    risk_level,
    coaching_certifications,
    non_icf_cert,
    certification_mbti,
    focus_areas,
    products):

  certification_tag_list = cert_map(coaching_certifications) + cert_map([non_icf_cert]) + cert_map([certification_mbti])

  query_params = {
    'user[first_name]': first_name,
    'user[last_name]': last_name,
    'user[time_zone]': time_zone or '',
    'user[email]': email,
    'user[phone]': phone,
    'user[title]': title,
    'user[organization_id]': organization_id,
    'user[coach_profile_attributes][tier]': tier,
    'user[coach_profile_attributes][primary]': primary,
    'user[coaching_language]': coaching_language,
    'roles[]': roles or [],
    'user[coach_profile_attributes][fountain_applicant_id]': fountain_applicant_id,
    'user[coach_profile_attributes][languages][]': languages or [],
    'user[coach_profile_attributes][member_levels][]': member_levels or [],
    'user[coach_profile_attributes][industries][]': industries or [],
    'user[coach_profile_attributes][risk_level]': risk_level,
    'user[coach_profile_attributes][certification_qualifications][]': certification_tag_list,
    'user[coach_profile_attributes][focus_qualifications][]': focus_areas or [],
    'user[coach_profile_attributes][product_qualifications][]': products or [],
  }

  return urllib.parse.urlencode(query_params, doseq=True)
$$
;

{% endmacro %}