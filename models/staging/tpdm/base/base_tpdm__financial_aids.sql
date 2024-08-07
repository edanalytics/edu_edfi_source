with financial_aids as (
    {{ source_edfi3('financial_aids') }}
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
        -- identity components
        {{ extract_descriptor('v:aidTypeDescriptor::string') }} as aid_type,
        v:beginDate::date                                       as begin_date,
        v:studentReference:studentUniqueId::string              as student_unique_id,
        -- non-identity components
        v:aidAmount::float                as aid_amount,
        v:aidConditionDescription::string as aid_condition_description,
        v:endDate::date                   as end_date,
        v:pellGrantRecipient::boolean     as is_pell_grant_recipient,
        -- references
        v:studentReference as student_reference,
        -- edfi extensions
        v:_ext as v_ext
    from financial_aids
)
select * from renamed
