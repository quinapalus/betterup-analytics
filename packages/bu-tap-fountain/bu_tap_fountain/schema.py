"""
Contains the Singer schemas as dictionaries
for consumption in the tap.



Create applicant schema
"""

applicant_string_fields = [
    # Basic info
    "location",
    "city",
    "city_custom", # Most recent version of city
    "country",
    "country_custom", # Most recent version of country
    "green_state",
    "email",
    "first_name",
    "last_name",
    "funnel",
    "stage",
    "id", # Fountain ID
    "name",
    "linkedin",
    "phone_number",
    "normalized_phone_number", # Consistent formatting
    "personal_email", # Store of the coaches personal email after update to BU gmail
    "primary_email_updated",
    "bu_gmail", # Set only after email has been updated
    "facetime", # Used to interview coaches back in the day
    "skype", # Used to interview coaches back in the day
    "referrer", # More common, recent
    "referrer_name",
    "tier", # Old notes about application quality
    "labels", # Empty array for all records
    "timezone", # Null for all records
    "time_zone",
    "background_checks",
    "document_signatures",
    "previous_applicant", # Had the applicant applied previously
    "where_did_you_find_out_about_this_opportunity",
    "how_did_they_hear_about_betterup",
    "hours_of_professional_coaching",
    "professional_coaching_hours",
    "supervised_clinical_hours",

    # Old application questions
    "statement_of_interest",
    "coaching_background_and_clients",
    "describe_coaching_style_in_three_words",
    "explain_coaching_to_a_new_client",
    "how_would_you_build_rapport_with_clients",
    "how_do_you_handle_disengaged_clients",
    "how_do_you_stay_updated_on_evidence_based_practices",
    "tell_us_something_youve_learned_recently",
    "anything_else",

    # Staffing attributes (see array_fields as well)
    "adv_deg_tier",
    "postgraduate_degree_field",
    "current_coursework",
    "completed_courses",
    "semesters_of_graduate_study",
    "coach_cert_tier",
    "coach_training_completed", # Mixed array values with "Yes/No" string
    "coach_training_institute",
    "coach_training_program",
    "non_icf_cert",
    "coach_hrs_tier",
    "counseling_hrs_tier",

    "professional_licenses", # Yes / No / In progress
    "professional_license_type",
    "corporate_or_professional_experience", # Yes / No
    "executive_positions", # only 4 records
    "specialty",
    "additional_coaching_techniques",
    "mbti_certified",

    # Other
    "notes", # assorted notes from hiring process
    "source", # less robust how_did_they_hear_about_betterup

    # DATA-642 additions
    "inspiring_matrix",
    "inspiring_application_tier",
    "thriving_matrix",
    "thriving_application_tier",
    "coach_interview_tier",
    "vip_status",
    "high_priority",

    # DATA-740 additions
    "coaching_credential",
    "additional_certifications",

    # DATA-749
    "coach_credential_tier",
    "corporate_leadership",
    "corporate_leadership_tier",
    "counseling_tier",
    "inspiring_degree_tier",
    "inspiring_thriving",
    "license_tier",
    "thriving_corporate_experience_tier",
    "thriving_degree_tier",
    # DATA-749
    "counseling_hours",
    "thriving_corporate_years_experience",
    "counseling_years",

    # Deprecated or low-quality
    "hours_of_supervised_coaching",
    "supervised_intervention_hours",
    "hours_of_clinical_work_or_therapy",
    "nonclinical_client_hours",
    "years_of_corporate_or_professional_experience",
    "data",
    "type",
    "location_region",
    "resume",
    "mbti",
    "primary_email",
    "sterling_dob",
    "sterling_report_id",
    "final_mock_cd",
    "final_mock_cd_r",
    "final_mock_link",
    "final_mock_score",
    "final_mock_score_r",
    "legal_name",
    "counseling_member_level",
    "written_interview_coaching_journey",
    "written_interview_coaching_philosophy",
    "written_interview_complete",
    "written_interview_evidence_based",
]

applicant_date_fields = ["created_at", "updated_at", "last_transitioned_at"]

applicant_boolean_fields = ["receive_automated_emails", "is_duplicate"]


applicant_array_fields = [
    "postgraduate_degrees",
    "coaching_certifications",
    "coaching_techniques",
    "coaching_industries",
    "coaching_languages",
    "focus_area",
    "member_level",
    "working_industries"
]


applicant_properties = {}
for field in applicant_string_fields:
    applicant_properties[field] = {"type": ["null", "string"]}

for field in applicant_date_fields:
    applicant_properties[field] = {"type": ["null", "string"], "format": "date-time"}

for field in applicant_boolean_fields:
    applicant_properties[field] = {"type": ["boolean"]}

for field in applicant_array_fields:
    applicant_properties[field] = {"type": ["null", "array"], "items": {"type": "string"}}

applicants = {
    "type": ["object"],
    "additionalProperties": False,
    "properties": applicant_properties,
}

"""
Create applicant transition history schema
"""

transition_properties = {
    "applicant_id": {"type": ["null", "string"]},
    "stage_id": {"type": ["null", "string"]},
    "stage_name": {"type": ["null", "string"]},
    "created_at": {"type": ["null", "string"], "format": "date-time"}
}

transitions = {
    "type": ["object"],
    "additionalProperties": False,
    "properties": transition_properties,
}

"""
Create funnel (opening) schema
"""

funnel_properties = {
    "id": {"type": ["null", "string"]},
    "title": {"type": ["null", "string"]},
    "active": {"type": ["boolean"]},
    "stages": {"type": ["null", "array"], "items": {"type": "string"}}
}

funnels = {
    "type": ["object"],
    "additionalProperties": False,
    "properties": funnel_properties,
}
