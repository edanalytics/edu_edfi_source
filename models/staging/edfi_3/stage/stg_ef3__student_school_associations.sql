with base_student_school_assoc as (
    select * from {{ ref('base_ef3__student_school_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_school') }},
        {{ gen_skey('k_school_calendar') }},
        {{ gen_skey('k_graduation_plan') }},
        base_student_school_assoc.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_student_school_assoc
),
-- school_year is technically optional, so infer it if it's null to avoid breaking downstream joins
with_inferred_school_year as (
    select 
        {{ edu_edfi_source.star('keyed', except=['school_year']) }},
        {{ edu_edfi_source.infer_school_year() }} as school_year
    from keyed
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='with_inferred_school_year',
            partition_by='k_student, k_school, entry_date', 
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
