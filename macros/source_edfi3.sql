{#- Created an is_deleted flag using deletes resources -#}
{% macro source_edfi3(resource, join_deletes=True) -%}

    {% if join_deletes %}
        select
            api_data.*,
            get_ignore_case(deletes_data.v, 'id')::string is not null as is_deleted,
            coalesce(deletes_data.pull_timestamp, api_data.pull_timestamp) as last_modified_timestamp

        from {{ source('raw_edfi_3', resource) }} as api_data

            left join {{ source('raw_edfi_3', '_deletes') }} as deletes_data
            on (
                deletes_data.name = '{{ resource | lower }}'
                and api_data.tenant_code = deletes_data.tenant_code
                and api_data.api_year = deletes_data.api_year
                and api_data.v:id::string = replace(get_ignore_case(deletes_data.v, 'id')::string, '-')
            )

    {% else %}
        select *, false as is_deleted, pull_timestamp as last_modified_timestamp
        from {{ source('raw_edfi_3', resource) }}

    {% endif %}

{%- endmacro %}
