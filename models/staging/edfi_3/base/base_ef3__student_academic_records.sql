with academic_records as (
    {{ source_edfi3('student_academic_records') }}
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
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:schoolYearTypeReference:schoolYear::int                     as school_year,
        v:studentReference:studentUniqueId::string                    as student_unique_id,
        {{ extract_descriptor('v:termDescriptor::string') }}          as academic_term,
        -- non-identity components
        v:cumulativeAttemptedCreditConversion::float  as cumulative_attempted_credit_conversion,
        v:cumulativeAttemptedCredits::float           as cumulative_attempted_credits,
        v:cumulativeEarnedCreditConversion::float     as cumulative_earned_credit_conversion,
        v:cumulativeEarnedCredits::float              as cumulative_earned_credits,
        v:projectedGraduationDate::date               as projected_graduation_date,
        v:sessionAttemptedCreditConversion::float     as session_attempted_credit_conversion,
        v:sessionAttemptedCredits::float              as session_attempted_credits,
        v:sessionEarnedCreditConversion::float        as session_earned_credit_conversion,
        v:sessionEarnedCredits::float                 as session_earned_credits,
        v:classRanking:classRank::float               as class_rank,
        v:classRanking:totalNumberInClass::float      as class_rank_total_students,
        v:classRanking:percentageRanking::float       as class_percent_rank,
        v:classRanking:classRankingDate::date         as class_rank_date,
        -- deprecated components (use gpa list instead)
        v:cumulativeGradePointAverage::float          as cumulative_gpa,
        v:cumulativeGradePointsEarned::float          as cumulative_grade_points_earned,
        v:gradeValueQualifier::string                 as grade_value_qualifier,
        v:sessionGradePointAverage::float             as session_gpa,
        v:sessionGradePointsEarned::float             as session_grade_points_earned,
        -- descriptors
        {{ extract_descriptor('v:sessionEarnedCreditTypeDescriptor::string') }}       as session_earned_credit_type,
        {{ extract_descriptor('v:sessionAttemptedCreditTypeDescriptor::string') }}    as session_attempted_credit_type,
        {{ extract_descriptor('v:cumulativeEarnedCreditTypeDescriptor::string') }}    as cumulative_earned_credit_type,
        {{ extract_descriptor('v:cumulativeAttemptedCreditTypeDescriptor::string') }} as cumulative_attempted_credit_type,
        -- references 
        v:studentReference               as student_reference,
        v:educationOrganizationReference as education_organization_reference,
        -- lists
        v:academicHonors     as v_academic_honors,
        v:diplomas           as v_diplomas,
        v:gradePointAverages as v_grade_point_averages,
        v:recognitions       as v_recognitions,
        v:reportCards        as v_report_cards,

        -- edfi extensions
        v:_ext as v_ext
    from academic_records
)
select * from renamed