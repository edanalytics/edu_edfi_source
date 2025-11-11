with base_student_iep_service_deliveries as (
    select * from {{ ref('base_sedm__student_iep_service_deliveries') }}
),

keyed as (
    select 
        {{ dbt_utils.generate_surrogate_key(
            [
                'tenant_code',
                'api_year',
                'lower(iep_service_delivery_id)',
                'lower(student_unique_id)',
                'lower(student_iep_association_id)',
                'lower(service_delivery)',
                'service_delivery_date'
            ]
        ) }} as k_student_iep_service_delivery,
        {{ gen_skey('k_student') }},
        {{ gen_skey('k_student_xyear') }},
        {{ gen_skey('k_student_iep_association') }},
        {{ gen_skey('k_student_iep_service_prescription') }},
        {{ gen_skey('k_staff', alt_ref='service_provider_staff_reference') }},
        api_year as school_year,
        base_student_iep_service_deliveries.*
        {{ extract_extension(model_name=this.name, flatten=True) }}

    from base_student_iep_service_deliveries
),

deduped as (
    {{ dbt_utils.deduplicate(
        relation='keyed',
        partition_by='k_student_iep_service_delivery',
        order_by='last_modified_timestamp desc, pull_timestamp desc'
    ) }}
)

select * from deduped
where not is_deleted
