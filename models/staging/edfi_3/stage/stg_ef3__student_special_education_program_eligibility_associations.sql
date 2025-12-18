with base_student_program_eligibility_associations as (
    select * from {{ ref('base_ef3__student_special_education_program_eligibility_associations') }}
),

keyed as (
    select

        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_program') }},
        {{ edorg_ref() }},

        api_year as school_year,
        base_student_program_eligibility_associations.*

        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_student_program_eligibility_associations
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_lea, k_school, k_student, k_program, consent_to_evaluation_received_date',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted
