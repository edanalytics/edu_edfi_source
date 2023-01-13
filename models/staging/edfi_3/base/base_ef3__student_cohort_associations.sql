with stu_cohort_assoc as (
    {{ edu_edfi_source.source_edfi3('student_cohort_associations') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        file_row_number,
        filename,
        is_deleted,
        v:id::string                                       as record_guid,
        v:studentReference:studentUniqueId::string         as student_unique_id,
        v:cohortReference:educationOrganizationId::integer as cohort_ed_org_id,
        v:cohortReference:cohortIdentifier::string         as cohort_id,
        v:beginDate::date                                  as cohort_begin_date,
        v:endDate::date                                    as cohort_end_date,
        -- references
        v:studentReference as student_reference,
        v:cohortReference  as cohort_reference,
        -- lists 
        v:sections as v_sections,

        -- edfi extensions
        v:_ext as v_ext

    from stu_cohort_assoc
)
select * from renamed
