    
{{ config(severity = 'warn') }}

{%- set DELAY_DELIVERY = check_delayed_delivery() | int -%}
{%- set SALESTATUSIND = check_homesite_saledate_ind() | int -%}
{%- set CANCEL_STATUS_IND = check_homesite_cancel_status_ind() | int -%} 

{% if SALESTATUSIND==0  and DELAY_DELIVERY==0 and CANCEL_STATUS_IND==0 %} 
        select 1 as temp where 1=2
{% else %}
        select 1 as temp 
{% endif %}