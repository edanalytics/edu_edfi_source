{% test relationship(model, parent, column_name) %} 
    {%- set all_columns = adapter.get_columns_in_relation(model) %}
    {%- set all_columns = all_columns | map(attribute='column') | list %}
    with child as (
        select {{ column_name }} as from_field
            {%-if 'TENANT_CODE' in all_columns %}
                ,tenant_code,
            {%- endif %}
            {%- if 'API_YEAR' in all_columns %}
                api_year,
            {%- endif %}
        from {{ model }}
        where {{ column_name }} is not null
    ),
    parent as (
        select {{ column_name }} as to_field
            {%-if 'TENANT_CODE' in all_columns %}
                ,tenant_code,
            {%- endif %}
            {%- if 'API_YEAR' in all_columns %}
                api_year,
            {%- endif %}
        from {{ref(parent) }}
    )
    select
        {%-if 'TENANT_CODE' in all_columns %}
        child.tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns %}
        child.api_year,
        {%- endif %}
        object_construct('test_column', array_construct('{{ column_name }}'), 'parent_model_name', '{{ parent }}' ) as test_params,
        count(*) as failed_row_count
    from child
    left join parent
        on child.from_field = parent.to_field
    where parent.to_field is null
    group by all
{% endtest %}



{% test unique_combination_of_columns(model, combination_of_columns) %}
    {%- set all_columns = adapter.get_columns_in_relation(model) %}
    {%- set all_columns = all_columns | map(attribute='column') | list %}

    with validation_errors as (
        select
            {%- for value in combination_of_columns %} {{value}},{%-endfor%}
        {%-if 'TENANT_CODE' in all_columns and 'tenant_code' not in combination_of_columns %}
            tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns and 'api_year' not in combination_of_columns %}
            api_year,
        {%- endif %}
        {%- if 'SCHOOL_YEAR' in all_columns and 'school_year' not in combination_of_columns %}
            school_year,
        {%- endif %}
            count(*) as failed_row_count
        from {{ model }}
        group by all
        having count(*) > 1

    )

    select distinct
        {%-if 'TENANT_CODE' in all_columns %}
            tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns %}
            api_year,
        {%- endif %}
        {%- if 'SCHOOL_YEAR' in all_columns %}
            school_year,
        {%- endif %}
            failed_row_count,
        object_construct('test_columns', array_construct({{combination_of_columns}}) ) as test_params
    from validation_errors

{% endtest %}


{% test accepted_values(model, values, column_name, quote_columns = true) %}
    {%- set all_columns = adapter.get_columns_in_relation(model) %}
    {%- set all_columns = all_columns | map(attribute='column') | list %}
    
    {%- if not quote %}
        {%- set accepted_list = [] %}
        {%- for value in values %}
            {%- if value is string and value[0] != "'" and value[-1] != "'" %}
                {%- set accepted_list = accepted_list.append("'" + value + "'") %}
            {%- else %}
                {%- set accepted_list = accepted_list.append(value) %}
            {%- endif %}
        {%- endfor %}
    {%- elif quote %}
        {%- set accepted_list = values %}
    {%- endif%}

    select 
         {%-if 'TENANT_CODE' in all_columns %}
        tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns %}
        api_year,
        {%- endif %},
        {%- if 'SCHOOL_YEAR' in all_columns %}
        school_year,
        {%- endif %}
        count(*) as failed_row_count,
        object_construct('accepted_values', {{values}} ) as test_params
    from {{ model }}
    where {{ column_name.strip('"') }} not in ( {%- for value in accepted_list %} {{value}} {%- if not loop.last %}, {%-else %} {%-endif%} {%-endfor%} )
    group by all
    having count(*) > 1

{% endtest%}


{% test not_null(model, column_name) %}
    {%- set all_columns = adapter.get_columns_in_relation(model) %}
    {%- set all_columns = all_columns | map(attribute='column') | list %}
    select 
        {%-if 'TENANT_CODE' in all_columns %}
        tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns %}
        api_year,
        {%- endif %}
        {%- if 'SCHOOL_YEAR' in all_columns %}
        school_year,
        {%- endif %}
        count(*) as failed_row_count,
        object_construct('test_column', array_construct('{{ column_name }}') ) as test_params
    from {{ model }}
    where {{ column_name }} is null
    group by all

{% endtest %}


{% test unique(model, column_name) %}
    {%- set all_columns = adapter.get_columns_in_relation(model) %}
    {%- set all_columns = all_columns | map(attribute='column') | list %}
    select 
        {{column_name}},
        {%-if 'TENANT_CODE' in all_columns %}
        tenant_code,
        {%- endif %}
        {%- if 'API_YEAR' in all_columns %}
        api_year,
        {%- endif %}
        {%- if 'SCHOOL_YEAR' in all_columns %}
        school_year,
        {%- endif %}
        count(*) as failed_row_count,
        object_construct('test_column', array_construct('{{ column_name }}') )  as test_params
    from {{ model }}
    where {{ column_name }} is not null
    group by all
    having count(*) > 1

{% endtest %}




