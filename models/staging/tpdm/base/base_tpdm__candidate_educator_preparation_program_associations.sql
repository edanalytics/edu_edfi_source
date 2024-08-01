with candidate_educator_preparation_program_associations as (
    {{ source_edfi3('candidate_educator_preparation_program_associations') }}
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
        v:beginDate::date                                                                       as begin_date,
        v:candidateReference:candidateIdentifier::string                                        as candidate_id,
        v:educatorPreparationProgramReference:educationOrganizationId::int                      as ed_org_id,
        v:educatorPreparationProgramReference:programName::string                               as program_name,
        {{ extract_descriptor('v:educatorPreparationProgramReference:programTypeDescriptor') }} as program_type,
        -- non-identity components
        v:educatorPreparationProgramReference:link:rel::string as ed_org_type,
        v:endDate::date                                        as end_date,
        -- descriptors
        {{ extract_descriptor('v:reasonExitedDescriptor::string') }}      as reason_exited,
        {{ extract_descriptor('v:eppProgramPathwayDescriptor::string') }} as epp_program_pathway,
        -- unnested lists
        v:degreeSpecializations as v_degree_specializations,
        v:cohortYears           as v_cohort_years,
        -- references
        v:candidateReference                  as candidate_reference,
        v:educatorPreparationProgramReference as educator_preparation_program_reference,
        -- edfi extensions
        v:_ext as v_ext
    from candidate_educator_preparation_program_associations
)
select * from renamed
