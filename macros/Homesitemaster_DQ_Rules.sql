

{% macro check_delayed_delivery() %}
{%- call statement('DELAY_DELIVERY', fetch_result=True) -%}
     SELECT * FROM "PC_DBT_DB"."DBT_ABASAK_CUST_DETAIL"."SAT_HOMESITE" WHERE ADDRESSLINE1 IS NOT NULL AND ADDRESSLINE1<>'NULL'
     AND ACTUALDELIVERYDATE>ESTIMATEDDELIVERYDATE AND SALEDATE<>'NULL' AND ACTUALDELIVERYDATE<>'NULL';
{%- endcall -%}
{% if execute %}
{%- set Query_count = load_result('DELAY_DELIVERY')['data'][0][0]  -%}
{{return (Query_count)}}
{% endif %}
{% endmacro %}


{% macro check_homesite_cancel_status_ind() %}
{%- call statement('CANCEL_STATUS_IND', fetch_result=True) -%}
     SELECT * FROM "PC_DBT_DB"."DBT_ABASAK_CUST_DETAIL"."SAT_HOMESITE" WHERE ishomesitecancelled='0'
     and cancellationdate <>'NULL'
{%- endcall -%}
{% if execute %}
{%- set Query_count = load_result('CANCEL_STATUS_IND')['data'][0][0]  -%}
{{return (Query_count)}}
{% endif %}
{% endmacro %}


{% macro check_homesite_saledate_ind() %}
{%- call statement('SALESTATUSIND', fetch_result=True) -%}      
SELECT * FROM "PC_DBT_DB"."DBT_ABASAK_CUST_DETAIL"."SAT_HOMESITE" WHERE ishomesitesold='0' and saledate <>'NULL'
and cancellationdate='NULL' and ishomesitecancelled='0'
{%- endcall -%}
{% if execute %}
{%- set Query_count = load_result('SALESTATUSIND')['data'][0][0]  -%}
{{return (Query_count)}}
{% endif %}
{% endmacro %}
select 1 as temp where 1=2