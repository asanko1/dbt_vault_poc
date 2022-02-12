{% test edna_str_len_check(model, column_name, desired_len,c_name) %}

    {% set name = 'len_chk__' ~ model.name ~ '__' ~ column_name %}

    {% set description %} Assert that {{ column_name }} has {{desired_len}} in {{ model }} {% endset %}

    {% set fail_msg %} Found {{ result }} length in {{ model }}.{{ column_name }} {% endset %}
        
    {{ config(name=name, description=description, fail_msg=fail_msg) }}
    
    select CDMHOMESITEID,HOMESITEID,   '{{c_name}}'||'-LENGTH ISSUE' AS ISSUE
    from {{ model }}
    where {{ column_name }} is not null 
    and length({{column_name}})<>{{desired_len}}

{% endtest %}

{% test edna_col_unique_check(model, column_name) %}
select
   CDMHOMESITEID,HOMESITEID,
    '{{column_name}}'||'_DUPLICATE_VALUE' AS ISSUE

from PC_DBT_DB.dbt_abasak.SAT_HomeSite
where {{column_name}} is not null
group by CDMHOMESITEID,HOMESITEID,{{column_name}}
having count(*) > 1
{% endtest %}