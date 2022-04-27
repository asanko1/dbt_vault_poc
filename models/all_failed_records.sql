{%- set v_sql = " " %}
{%- call statement('FAIL_RESULT_TABLES_CNT', fetch_result=True) -%}
    select count("table_name") as table_name from (
    {{dbt_utils.get_tables_by_prefix_sql('DBT_ABASAK_DBT_TEST__AUDIT', 'EDNA')}}
    union
    {{dbt_utils.get_tables_by_prefix_sql('DBT_ABASAK_DBT_TEST__AUDIT', 'DBT_UTILS')}}
    )
{%- endcall -%}
{% if execute %}
    {%- set table_list_count = load_result('FAIL_RESULT_TABLES_CNT')['data'][0][0] | int -%}
    {%- call statement('FAIL_RESULT_TABLES', fetch_result=True) -%}
        select "table_schema"||'.'||"table_name"  from (
        {{dbt_utils.get_tables_by_prefix_sql('DBT_ABASAK_DBT_TEST__AUDIT', 'EDNA')}}
        union
        {{dbt_utils.get_tables_by_prefix_sql('DBT_ABASAK_DBT_TEST__AUDIT', 'DBT_UTILS')}}
        )
    {%- endcall -%}
    {% if execute %}
        {%- set table_name = load_result('FAIL_RESULT_TABLES')['data'][0][0]  -%}
        {% for i in range(table_list_count) %}
            {%- set table_name = load_result('FAIL_RESULT_TABLES')['data'][i][0]  -%}
                select * from {{table_name}}  union               
        {% endfor %}
        select * from {{table_name}}
    {% endif %}
{% endif %}



--adding comment
