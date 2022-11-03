with base_academic_records as (
    select * from {{ ref('base_ef3__student_academic_records') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.surrogate_key(
            [
                'tenant_code',
                'ed_org_id',
                'school_year',
                'student_unique_id',
                'lower(academic_term)'
            ]
        ) }} as k_student_academic_record,
        {{ gen_skey('k_student') }}, --todo: or k_student_xyear?
        -- todo: can we use ed_org_ref to generate either a k_school or k_lea here
        -- note: appears to be driven by the same logic as student_ed_org_assoc, so could use a single variable for both
        {{ edorg_ref(annualize=False) }},
        base_academic_records.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_academic_records
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_academic_record, ed_org_id',
            order_by='pull_timestamp desc'
        )
    }}
)
select * from deduped