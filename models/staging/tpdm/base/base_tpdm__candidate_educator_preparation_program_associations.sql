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

        v:id::string                                             as record_guid,
        v:candidateReference:candidateIdentifier::string         as candidate_id,
        v:educatorPreparationProgramReference:programId::string  as program_id,
        v:applicationReference:applicationIdentifier::string     as application_id,
        v:beginDate::date                                        as begin_date,
        v:endDate::date                                          as end_date,
        -- descriptors
        {{ extract_descriptor('v:reasonExitedDescriptor::string') }}      as reason_exited,
        {{ extract_descriptor('v:eppProgramPathwayDescriptor::string') }} as epp_program_pathway,
        -- unnested lists
        v:identificationCodes   as v_identification_codes,
        v:candidateIndicators   as v_candidate_indicators,
        v:degreeSpecializations as v_degree_specializations,
        v:cohortYears           as v_cohort_years,
        -- references
        v:candidateReference                  as candidate_reference,
        v:educatorPreparationProgramReference as educator_preparation_program_reference,
        v:applicationReference                as application_reference
    from candidate_educator_preparation_program_associations
)

select * from renamed
