{%- call statement('DQ_RESULT', fetch_result=True) -%}
    UPDATE "PC_DBT_DB"."DBT_DQ_TEST"."DQ_CONFIG" SET OUTPUT_RESULT = -1
{%- endcall -%}

{%- set Query_Count = get_query_count() | int -%}
{%- call statement('DQ_CONFIG', fetch_result=True) -%}
    SELECT  Query,ID FROM "PC_DBT_DB"."DBT_DQ_TEST"."DQ_CONFIG" where active_flg='1'
{%- endcall -%}

{% for i in range(Query_Count) %}
    {%- set Query_statement = load_result('DQ_CONFIG')['data'][i][0] -%}
    {%- set Query_ID = load_result('DQ_CONFIG')['data'][i][1] -%}

    {%- call statement('DQ_RESULT', fetch_result=True) -%}
    UPDATE "PC_DBT_DB"."DBT_DQ_TEST"."DQ_CONFIG" SET OUTPUT_RESULT = (SELECT * FROM ({{Query_statement}}))
    WHERE ID={{Query_ID}} 
    {%- endcall -%}
{% endfor %}

{%- call statement('DQ_RESULT_VALIDATE', fetch_result=True) -%}
    SELECT count(*) from "PC_DBT_DB"."DBT_DQ_TEST"."DQ_CONFIG" WHERE  OUTPUT_RESULT <> EXPECTED_RESULT
{%- endcall -%}

 {%- set DQ_Issue = load_result('DQ_RESULT_VALIDATE')['data'][0][0] -%}

    {% if DQ_Issue==0  %} 

            select 1 as temp where 1=2
    {% else %}
            select 1 as temp 
     {% endif %}
    
