with base_staff_school_assoc as (
    select * from {{ ref('base_ef3__staff_school_associations') }}
),
keyed as (
    select 
        {{ gen_skey('k_staff') }},
        {{ gen_skey('k_school') }},
        {{ gen_skey('k_school_calendar') }},
        -- note: neither school_year nor the calendar association are required,
        -- which means this table almost never contains year data, so we have to 
        -- add it back in where possible
        coalesce(school_year, api_year) as school_year,
        {{ dbt_utils.star(from=ref('base_ef3__staff_school_associations'), except=['school_year']) }}
        {{ extract_extension(model_name=this.name, flatten=True) }}
    from base_staff_school_assoc

),
deduped as (
    {{
        dbt_utils.deduplicate(
            relation='keyed',
            partition_by='k_staff, k_school, program_assignment, school_year',
            order_by='last_modified_timestamp desc, pull_timestamp desc'
        )
    }}
)
select * from deduped
where not is_deleted
