with source_student_iep_goals as (
    {{ source_edfi3('student_iep_goals') }}
),

renamed as (
    select
        -- generic columns
        tenant_code,
        api_year,
        pull_timestamp,
        last_modified_timestamp,
        file_row_number,
        filename,
        is_deleted,

        v:id::string                                                                      as record_guid,
        ods_version,
        data_model_version,
        
        -- identity components (composite key)
        v:iepGoalID::string                                                               as iep_goal_id,
        v:studentReference:studentUniqueId::string                                        as student_unique_id,
        v:studentIEPReference:studentIEPAssociationID::string                             as student_iep_association_id,
        v:studentIEPReference:educationOrganizationReference:educationOrganizationId::int as iep_servicing_ed_org_id,
        v:studentIEPReference:iepFinalizedDate::date                                      as iep_finalized_date,
        
        -- required fields
        {{ extract_descriptor('v:iepGoalDescriptor::string') }}                           as iep_goal,
        v:iepGoalDetails::string                                                          as iep_goal_details,
        
        -- optional fields
        v:goalAchievementPeriodBeginDate::date                                            as goal_achievement_period_begin_date,
        v:goalAchievementPeriodEndDate::date                                              as goal_achievement_period_end_date,
        
        -- references
        v:studentReference                                                                as student_reference,
        v:studentIEPReference                                                             as student_iep_reference,
        v:ideaeventReferences                                                             as ideaevent_references,
        
        -- edfi extensions
        v:_ext                                                                            as v_ext

    from source_student_iep_goals
)

select * from renamed
