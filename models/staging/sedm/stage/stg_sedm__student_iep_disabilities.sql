with base as (
    select * from {{ ref('base_sedm__student_iep_disabilities') }}
),
keyed as (
    select
        {{ gen_skey('k_student_iep') }},
        {{ edorg_ref(annualize=True) }},
        api_year as school_year,
        base.*,
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_student_iep, ed_org_id',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
),
flattened as (
    select
        tenant_code,
        school_year,
        k_student_iep,
        ed_org_id,
        {{ extract_descriptor('value:disabilityDescriptor::string') }} as disability_descriptor,
        {{ extract_descriptor('value:disabilityDeterminationSourceTypeDescriptor::string') }} as disability_determination_source_type_descriptor,
        {{ extract_descriptor('value:disabilityDiagnosis::string') }} as disability_diagnosis,
        {{ extract_descriptor('value:orderOfDisability::int') }} as order_of_disability
    from deduped
        {{ json_flatten('v_disabilities') }}
    where not is_deleted
)