with base_courses as (
    select * from {{ ref('base_ef3__courses') }}
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'lower(course_code)',
            'ed_org_id']
        ) }} as k_course, 
        {{ edorg_ref() }},
        api_year as school_year,
        base_courses.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_courses
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_course',
            order_by='last_modified_timestamp desc, pull_timestamp desc')
    }}
)
select * from deduped
where not is_deleted
order by tenant_code, api_year desc, ed_org_id, course_code
