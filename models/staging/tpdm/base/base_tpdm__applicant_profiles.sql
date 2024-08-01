with applicant_profiles as (
    {{ source_edfi3('applicant_profiles') }}
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
        v:applicantProfileIdentifier::string as applicant_profile_id,
        -- non-identity components
        v:firstName::string   as first_name,
        v:lastSurname::string as last_name,
        -- descriptors
        {{ extract_descriptor('v:sexDescriptor::string') }} as sex,
        -- unnested lists
        v:addresses                       as v_addresses,
        v:applicantCharacteristics        as v_applicant_characteristics,
        v:backgroundChecks                as v_background_checks,
        v:disabilities                    as v_disabilities,
        v:educatorPreparationProgramNames as v_educator_preparation_program_names,
        v:electronicMails                 as v_electronic_mails,
        v:gradePointAverages              as v_grade_point_averages,
        v:highlyQualifiedAcademicSubjects as v_highly_qualified_academic_subjects,
        v:identificationDocuments         as v_identification_documents,
        v:internationalAddresses          as v_international_addresses,
        v:languages                       as v_languages,
        v:personalIdentificationDocuments as v_personal_identification_documents,
        v:races                           as v_races,
        v:telephones                      as v_telephones,
        v:visas                           as v_visas,
        -- edfi extensions
        v:_ext as v_ext
    from applicant_profiles
)
select * from renamed
