{{ 
    config(
        materialized='table',
        tags=["Source_system_inventory2"]
        )
}}
{%- set column_names = ['HOMESITEID']    -%}
{% for column_name in column_names %}

       select BATCH_ID,MODEL_NAME,'HOMESITEID' AS KEYNAME,HOMESITEID AS KEYVALUE, 
        '{{column_name}}' AS FIELDANME, {{column_name}} as FIELDVALUE, 
       '{{column_name}}'||'-DUPLICATE_VALUE' AS ISSUE , 'Error' as ErrorClassification from 
        (select BATCH_ID,MODEL_NAME,{{column_name}}
            FROM {{ref('SAT_HomeSite')}}
           where {{ column_name }} is not null
           group by BATCH_ID,MODEL_NAME,{{column_name}}
           having count(*) > 1 ) union all 
    
{% endfor %} 
-- check for length 
{%- set column_length_check = {'PLANCODE':3 }   -%}

{% for column_name, desired_len in column_length_check.items() %}
     
       select BATCH_ID,MODEL_NAME,'HOMESITEID' AS KEYNAME,HOMESITEID AS KEYVALUE,
        '{{column_name}}' AS FIELDANME, {{column_name}} as FIELDVALUE,
       '{{column_name}}'||'-LENGTH ISSUE' AS ISSUE , 'Error' as  ErrorClassification 
           FROM {{ref('SAT_HomeSite')}}
           where {{ column_name }} is not null 
           and length({{column_name}})<>{{desired_len}} union all 
    
{% endfor %}
-- check for valid templateno 
select BATCH_ID,MODEL_NAME,'HOMESITEID' AS KEYNAME,HOMESITEID AS KEYVALUE,
'TEMPLATENUMBER' AS FIELDANME, TEMPLATENUMBER as FIELDVALUE, 'INVALID Template NO','W' from {{ref('SAT_HomeSite')}}
where ( TEMPLATENUMBER>190 and TEMPLATENUMBER<200 ) and
TEMPLATENUMBER <> NULL  
union all 
 SELECT BATCH_ID,MODEL_NAME,'HOMESITEID' AS KEYNAME,HOMESITEID AS KEYVALUE,
 'ACTUALDELIVERYDATE' AS FIELDANME, ACTUALDELIVERYDATE as FIELDVALUE,'DELAY_DELIVERY','W' 
        FROM {{ref('SAT_HomeSite')}}
        WHERE ADDRESSLINE1 IS NOT NULL AND ADDRESSLINE1<>'NULL'
     AND ACTUALDELIVERYDATE > ESTIMATEDDELIVERYDATE AND SALEDATE<>'NULL' AND ACTUALDELIVERYDATE<>'NULL'
 union all 
    SELECT  BATCH_ID,MODEL_NAME,'HOMESITEID' AS KEYNAME,HOMESITEID AS KEYVALUE,
     'ishomesitecancelled' AS FIELDANME, ishomesitecancelled as FIELDVALUE, 'CANCEL_STATUS_IND' ,'W' 
     FROM {{ref('SAT_HomeSite')}} 
     WHERE ishomesitecancelled='0'
     and cancellationdate <>'NULL'
 union all
     SELECT  BATCH_ID,MODEL_NAME,'HOMESITEID' AS KEYNAME,HOMESITEID AS KEYVALUE,
      'ishomesitesold' AS FIELDANME, ishomesitesold as FIELDVALUE, 'SALESTATUSIND' ,'W' 
     FROM {{ref('SAT_HomeSite')}}
     WHERE ishomesitesold='0' and saledate <>'NULL'
     and cancellationdate='NULL' and ishomesitecancelled='0'         

