with students as (
    select * from {{ ref('stg_ef3__students') }}
),
flattened as (
    select 
        tenant_code,
        api_year,
        k_student,
        k_student_xyear,
        pull_timestamp,
        last_modified_timestamp,
        is_deleted,
        {{ extract_descriptor('value:otherNameTypeDescriptor::varchar') }} as otherNameType,
        value:personalTitlePrefix::varchar as personalTitlePrefix,
        value:firstName::varchar as firstName,
        value:middleName::varchar as middleName,
        value:lastSurname::varchar as lastSurname,
        value:generationCodeSuffix::varchar as generationCodeSuffix
    from students
        {{ json_flatten('v_other_names') }}
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='flattened',
            partition_by='k_student, otherNameType', 
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
) 
select * from deduped
where not is_deleted