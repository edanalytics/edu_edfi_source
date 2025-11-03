with source_iep_goals as (
    {{ source_edfi3('student_iep_goals') }}
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
        -- key columns
        v:iepGoalID::string                                                               as iep_goal_id,
        v:studentReference:studentUniqueId::string                                        as student_unique_id,
        v:studentIEPReference:studentIEPAssociationID::string                             as student_iep_association_id,
        -- value columns
        v:iepGoalDetails::string               as iep_goal_details,
        v:goalAchievementPeriodBeginDate::date as goal_achievement_period_begin_date,
        v:goalAchievementPeriodEndDate::date   as goal_achievement_period_end_date,
        -- descriptors
        {{ extract_descriptor('v:iepGoalDescriptor::string') }} as iep_goal,
        -- references
        v:studentReference    as student_reference,
        v:studentIEPReference as student_iep_reference,
        v:ideaeventReferences as ideaevent_references,
        
        -- edfi extensions
        v:_ext as v_ext

    from source_student_iep_goals
)

select * from renamed
