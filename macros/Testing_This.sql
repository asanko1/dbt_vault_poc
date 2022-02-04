{% macro testthis() %}
    insert into ABC.Public.TestThis (ColName)
    Select '{{this}}'
{% endmacro %}
