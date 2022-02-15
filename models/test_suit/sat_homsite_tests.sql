{{ 
    config(
        materialized='table',
        tags=["Source_system_inventory2"]
        )
}}
{%- set column_names = ['HOMESITEID','CITY']    -%}
{% for column_name in column_names %}

       select BATCH_ID,MODEL_NAME,{{column_name}}, 
       '{{column_name}}'||'-DUPLICATE_VALUE' AS ISSUE , 'Error' as ErrorClassification from 
        (select BATCH_ID,MODEL_NAME,CDMHOMESITEID,{{column_name}}
            FROM {{ref('SAT_HomeSite')}}
           where {{ column_name }} is not null
           group by BATCH_ID,MODEL_NAME,CDMHOMESITEID,{{column_name}}
           having count(*) > 1 ) union all 
    
{% endfor %} 
-- check for length 
{%- set column_length_check = {'PLANCODE':3 }   -%}

{% for column_name, desired_len in column_length_check.items() %}
     
       select BATCH_ID,MODEL_NAME,{{column_name}}, 
       '{{column_name}}'||'-LENGTH ISSUE' AS ISSUE , 'Error' as  ErrorClassification 
           FROM {{ref('SAT_HomeSite')}}
           where {{ column_name }} is not null 
           and length({{column_name}})<>{{desired_len}}
    {% if not loop.last %}union all {% endif %}
{% endfor %}

