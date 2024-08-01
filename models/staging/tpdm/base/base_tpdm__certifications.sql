with certifications as (
    {{ source_edfi3('certifications') }}
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
        v:certificationIdentifier::string as certification_id,
        v:namespace::string               as namespace,
        -- non-identity components
        v:certificationTitle::string      as certification_title,
        -- descriptors
        {{ extract_descriptor('v:certificationFieldDescriptor::string') }}    as certification_field,
        {{ extract_descriptor('v:certificationLevelDescriptor::string') }}    as certification_level,
        {{ extract_descriptor('v:certificationStandardDescriptor::string') }} as certification_standard,
        {{ extract_descriptor('v:educatorRoleDescriptor::string') }}          as educator_role,
        {{ extract_descriptor('v:instructionalSettingDescriptor::string') }}  as instructional_setting,
        {{ extract_descriptor('v:minimumDegreeDescriptor::string') }}         as minimum_degree,
        {{ extract_descriptor('v:populationServedDescriptor::string') }}      as population_served,
        -- unnested lists
        v:certificationExams as v_certification_exams,
        v:gradeLevels        as v_grade_levels,
        v:routes             as v_routes
    from certifications
)
select * from renamed
