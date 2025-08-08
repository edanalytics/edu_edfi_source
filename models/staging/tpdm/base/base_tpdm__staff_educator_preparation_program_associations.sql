with staff_educator_preparation_program_associations as (
    {{ source_edfi3('staff_educator_preparation_program_associations') }}
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
        v:staffReference:staffUniqueId::string                                                  as staff_id,
        v:educatorPreparationProgramReference:educationOrganizationId::int                      as ed_org_id,
        v:educatorPreparationProgramReference:programName::string                               as program_name,
        {{ extract_descriptor('v:educatorPreparationProgramReference:programTypeDescriptor') }} as program_type,
        -- non-identity components
        v:beginDate::date                                                                       as begin_date,
        v:educatorPreparationProgramReference:link:rel::string as ed_org_type,  -- this was declareed in base_tpdm__candidate_educator_preparation_program_associations, is it correct?
        v:endDate::date                                        as end_date,
        v:completer::boolean                                   as is_completer,
        -- references
        v:staffReference                      as staff_reference,
        v:educatorPreparationProgramReference as educator_preparation_program_reference,
        -- edfi extensions
        v:_ext as v_ext
    from staff_educator_preparation_program_associations
)
select * from renamed
