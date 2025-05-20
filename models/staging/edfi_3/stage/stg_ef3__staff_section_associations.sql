with base_staff_section_assoc as (
    select * from {{ ref('base_ef3__staff_section_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_staff') }},
        {{ gen_skey('k_course_section') }},
        base_staff_section_assoc.*,
        -- prior to 5.0, begin date was not part of the key, so should not be used in deduplication
        -- after 5.0, begin date should be used in deduplication
        case
            when base_staff_section_assoc.data_model_version < '5' then null
            else begin_date
        end as begin_date_key
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_staff_section_assoc
),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_staff, k_course_section, begin_date_key',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
