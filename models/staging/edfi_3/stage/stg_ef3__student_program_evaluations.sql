with base_student_program_evaluations as (
  select * from {{ ref("base_ef3__student_program_evaluations") }}
),

keyed as (
    select
        {{ gen_skey("k_program_evaluation") }},
        {{ gen_skey("k_student_xyear") }},
        {{ gen_skey("k_student") }},
        {{ gen_skey(
            "k_staff",
            alt_ref="staff_evaluator_staff_reference",
            alt_k_name="k_evaluator_staff"
        ) }},
        {{ edorg_ref() }},

        base_student_program_evaluations.*,

        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_student_program_evaluations
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation="keyed",
        partition_by="k_program_evaluation, k_student, evaluation_date",
        order_by="last_modified_timestamp desc, pull_timestamp desc"
    ) }}
)

select * from deduped
where not is_deleted
