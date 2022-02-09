{% test str_len_check(model, column_name, desired_len) %}

    {% set name = 'len_chk__' ~ model.name ~ '__' ~ column_name %}

    {% set description %} Assert that {{ column_name }} has {{desired_len}} in {{ model }} {% endset %}

    {% set fail_msg %} Found {{ result }} length in {{ model }}.{{ column_name }} {% endset %}
        
    {{ config(name=name, description=description, fail_msg=fail_msg) }}
    
    select *
    from {{ model }}
    where {{ column_name }} is not null 
    and length({{column_name}})<>{{desired_len}}

{% endtest %}