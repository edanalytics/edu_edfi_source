with base_students as (
    select * from {{ ref('base_ef3__students') }}
),
keyed as (
    select
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(student_unique_id)'
            ]
        ) }} as k_student,
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'lower(student_unique_id)'
            ]
        ) }} as k_student_xyear,
        base_students.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_students
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student', 
            order_by='last_modified_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
