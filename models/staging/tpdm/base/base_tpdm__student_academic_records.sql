with student_academic_records as (
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
        v:id::string                                                  as record_guid,

        v:studentReference:studentUniqueId::string                    as student_unique_id,
        v:educationOrganizationReference:educationOrganizationId::int as ed_org_id,
        v:educationOrganizationReference:link:rel::string             as ed_org_type,
        v:schoolYearTypeReference:schoolYear::int                     as school_year,
        v:projectedGraduationDate::date                          as projected_graduation_date,
        -- descriptors
        {{ extract_descriptor('v:termDescriptor::string') }}  as term,
        -- unflattened lists
        v:cumulativeEarnedCredits    as v_cumulative_earned_credits,
        v:cumulativeAttemptedCredits as v_cumulative_attempted_credits,
        v:classRanking               as v_class_ranking,
        v:academicHonors             as v_academic_honors,
        v:recognitions               as v_recognitions,
        v:sessionEarnedCredits       as v_session_earned_credits,
        v:sessionAttemptedCredits    as v_session_attempted_credits,
        v:gradePointAverages         as v_grade_point_averages,
        v:diplomas                   as v_diplomas,
        v:reportCards                as v_report_cards,
        -- references
        v:studentReference               as student_reference,
        v:educationOrganizationReference as ed_org_reference
    from student_academic_records
)

select * from renamed
