with base_goals as (
    select * from {{ source_edfi3('goals') }}
),
renamed as (
    select
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        filename,
        file_row_number,
        is_deleted,

        v:id::string as record_guid,
        -- identity components
        v:personReference:personId::string               as person_id,
        v:personReference:sourceSystemDescriptor::string as person_source_system,
        v:goalTitle::string                              as goal_title,
        v:assignmentDate::date                           as assignment_date,
        -- non-identity components
        v:goalDescription::string      as goal_description,
        v:comments::string             as comments,
        v:completed_indicator::boolean as is_completed,
        v:completedDate::date          as completed_date,
        v:dueDate::date                as due_date,
        -- descriptors
        {{ extract_descriptor('v:goalTypeDescriptor::string') }} as goal_type,
        -- references
        v:evaluationElementReference   as evaluation_element_reference,
        v:evaluationObjectiveReference as evaluation_objective_reference,
        v:parentGoalReference          as parent_goal_reference,
        v:personReference              as person_reference,
        -- edfi extensions
        v:_ext as v_ext
    from base_goals
)
select * from renamed