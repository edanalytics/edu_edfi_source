with base_course_offerings as (
    select * from {{ ref('base_ef3__course_offerings') }}
    where not is_deleted
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
             'lower(local_course_code)',
             'school_id',
             'school_year',
             'lower(session_name)']
        ) }} as k_course_offering,
        {{ gen_skey('k_school') }},
        {{ gen_skey('k_session') }},
        {{ gen_skey('k_course') }}, 
        base_course_offerings.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_course_offerings
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_course_offering',
            order_by='school_year desc, pull_timestamp desc'
        )
    }}
)
select * from deduped

