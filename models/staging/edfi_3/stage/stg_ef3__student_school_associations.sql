with base_student_school_assoc as (
    select * from {{ ref('base_ef3__student_school_associations') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(student_unique_id)',
            'lower(school_id)',
            'entry_date']
        ) }} as k_enrollment,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_school') }},
        {{ gen_skey('k_school_calendar') }},
        {{ gen_skey('k_graduation_plan') }},
        base_student_school_assoc.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_school_assoc
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_enrollment', 
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
