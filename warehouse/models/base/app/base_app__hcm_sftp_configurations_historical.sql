with archived_hcm_sftp_configurations as (

    select * from {{ source('app_archive', 'hcm_sftp_configurations') }}

)

select * from archived_hcm_sftp_configurations