with fieldwork_experiences as (
    select * from {{ ref('base_tpdm__fieldwork_experiences') }}
    where not is_deleted
),
keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            ['tenant_code',
            'api_year',
            'begin_date',
            'lower(fieldwork_id)',
            'lower(student_unique_id)']
        ) }} as k_fieldwork_experience,
        {{ gen_skey('k_educator_prep_program') }},
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        fieldwork_experiences.*
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from fieldwork_experiences
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_fieldwork_experience',
            order_by='pull_timestamp desc')
    }}
)
select * from deduped
