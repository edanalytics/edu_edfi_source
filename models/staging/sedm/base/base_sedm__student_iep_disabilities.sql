with disabilities as (
    {{ source('student_iep_disability_collections')}}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string as record_guid,
        ods_version,
        data_model_version,
        v:studentIEPReference as student_iep_reference,
        v:disabilities as v_disabilities,

        -- edfi extensions
        v:_ext as v_ext
    from disabilities
)
select * from renamed