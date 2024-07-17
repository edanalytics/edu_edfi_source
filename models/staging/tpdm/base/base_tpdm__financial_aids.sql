with student_aid as (
    {{ source_edfi3('student_aids') }}
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

        v:id::string                               as record_guid,
        v:studentReference:studentUniqueId::string as student_id,
        v:aidTypeDescriptor::string                as aid_type,
        v:beginDate::date                          as begin_date,
        v:aidAmount::decimal(19, 4)                as aid_amount,
        v:aidConditionDescription::string          as aid_condition_description,
        v:endDate::date                            as end_date,
        v:pellGrantRecipient::boolean              as pell_grant_recipient,
        -- references
        v:studentReference as student_reference
    from student_aid
)
select * from renamed