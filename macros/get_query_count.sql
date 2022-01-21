{% macro get_query_count() %}

{%- call statement('DQ_CONFIG_CNT', fetch_result=True) -%}
    SELECT count(Query) FROM "PC_DBT_DB"."DBT_DQ_TEST"."DQ_CONFIG" where active_flg='1'
{%- endcall -%}
{% if execute %}
{%- set Query_count = load_result('DQ_CONFIG_CNT')['data'][0][0]  -%}
{{return (Query_count)}}
{% endif %}


{% endmacro %}

select 1 as temp where 1=2