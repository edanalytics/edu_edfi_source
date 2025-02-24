with graduation_plans as (
    {{ source_edfi3('graduation_plans') }}
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
        v:educationOrganizationReference:educationOrganizationId::int           as ed_org_id,
        v:educationOrganizationReference:link:rel::string                       as ed_org_type,
        v:graduationSchoolYearTypeReference:schoolYear::int                     as graduation_school_year,
        {{ extract_descriptor('v:graduationPlanTypeDescriptor::string') }}      as graduation_plan_type,
        {{ extract_descriptor('v:totalRequiredCreditTypeDescriptor::string') }} as total_required_credit_type,
        v:totalRequiredCreditConversion::float                                  as total_required_credit_conversion,
        v:totalRequiredCredits::float                                           as total_required_credits,
        v:individualPlan::boolean                                               as is_individual_plan,
        -- lists
        v:creditsByCreditCategories as v_credits_by_credit_categories,
        v:creditsByCourses          as v_credits_by_courses,
        v:creditsBySubjects         as v_credits_by_subjects,   
        v:requiredAssessments       as v_required_assessments,
        --references
        v:educationOrganizationReference    as education_organization_reference,

        -- edfi extensions
        v:_ext as v_ext
    from graduation_plans
)
select * from renamed