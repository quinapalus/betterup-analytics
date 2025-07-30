WITH viva_user_profiles AS (

  SELECT * FROM {{ source('wkfw', 'viva_user_profiles') }}

)


SELECT
  id AS viva_user_profiles_id,
  user_id,
  {{ get_json_object('active_directory_profile_attributes', '"mail"') }} AS user_email,
  {{ get_json_object('active_directory_profile_attributes', '"surname"') }} AS surname,
  {{ get_json_object('active_directory_profile_attributes', '"jobTitle"') }} AS job_title,
  {{ get_json_object('active_directory_profile_attributes', '"givenName"') }} AS given_name,
  {{ get_json_object('active_directory_profile_attributes', '"department"') }} AS department,
  {{ get_json_object('active_directory_profile_attributes', '"employeeId"', 'varchar') }} AS employee_id,
  {{ get_json_object('active_directory_profile_attributes', '"displayName"') }} AS display_name,
  {{ get_json_object('active_directory_profile_attributes', '"organization_profile"[0]:"displayName"') }} AS organization_name,
  {{ get_json_object('active_directory_profile_attributes', '"officeLocation"') }} AS office_location,
  {{ get_json_object('active_directory_profile_attributes', '"preferredLanguage"') }} AS preferred_language,
  {{ load_timestamp('created_at') }},
  {{ load_timestamp('updated_at') }}
FROM viva_user_profiles
