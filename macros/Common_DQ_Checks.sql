{% test string_length_check(model, column_name, desired_len) %}

    select *
    from {{ model }}
    where {{ column_name }} is not null 
    and length({{column_name}})<>{{desired_len}}

{% endtest %}