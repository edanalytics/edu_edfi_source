with base as (
    select * from {{ ref('base_ef3__student_assessment_education_organizations') }}
    where not is_deleted
),


keyed as (
    select
        {{
            dbt_utils.generate_surrogate_key(
                [   'tenant_code',
                    'api_year',
                    'ed_org_identifier',
                    'ed_org_association_type',
                    'assessment_identifier',
                    'student_assessment_identifier',
                    'student_unique_id',
                    'lower(namespace)'
                ]
            )
        }} as k_student_assessment_ed_org,
        {{ gen_skey('k_ed_org') }},
        {{ gen_skey('k_student_assessment') }}
    from base
),


deduped as (
    {{
        dbt_utils.deduplicate(
            relation = 'keyed',
            partition_by = 'k_student_assessment_ed_org',
            order_by = 'pull_timestamp desc'
        )
    }}
)


select * from deduped
