
{{ 
    config(
        materialized='table',
        tags=["Source_system_orders"]
        )
}}

-- The below state ment job_id_check is the query that checks if the job id that is created with the incoming batch is new or a rerun
-- if the output of the query returns 'True' that means it is a new batch and the model will run
-- if the output of the query is 'False' then model is in re run and further check is required

{%- call statement('Job_Id_check', fetch_result=True) -%}
Select distinct 'True'  
where 0 =(
select count(*) from 
ABC.PUBLIC.ABC_Job_Details where   job_id = md5(concat(to_varchar('{{var('batch_id')}}'),'-','{{this}}'))
    )
union
Select distinct 'False'  
where 0 !=(
select count(*) from 
ABC.PUBLIC.ABC_Job_Details where   job_id = md5(concat(to_varchar('{{var('batch_id')}}'),'-','{{this}}'))
    ) ;

{%- endcall -%}
{%- set  Job_Id_status = load_result('Job_Id_check') ['data'][0][0]  -%}

{% set query -%}
            Insert into PC_DBT_DB.DBT_ABASAK_CUST_DETAIL.job_id(Job_ID) 
            VALUES ('{{Job_Id_status}}') ;
{%- endset %}
{% do run_query(query) %}



{% if Job_Id_status ==  'False'  %} 
-- if the model is in re run then we need to see if the previous ru was successful or not
-- if previous run is success then the data in snowflake should be as is (the else part does this)
-- if previous run has failed then we need to run the model again 

{%- call statement('Job_status', fetch_result=True) -%}
        Select  JOB_STATUS from ABC.PUBLIC.ABC_Job_Details  where job_id = md5(concat(to_varchar('{{var('batch_id')}}'),'-','{{this}}')) order by END_TIMESTAMP desc limit 1 ;
{%- endcall -%}
{%- set  Job_status_output = load_result('Job_status') ['data'][0][0]  -%}

{% set query -%}
            Insert into PC_DBT_DB.DBT_ABASAK_CUST_DETAIL.job_id(Job_ID) 
            VALUES ('{{Job_status_output}}') ;
{%- endset %}
{% do run_query(query) %}
{% else %}
{%- set  Job_status_output = 'NA'  -%}
{% endif %}


-- JOB ID Check 



{% if Job_Id_status ==  'True' or Job_status_output == 'FAILED' %} 

    {%- call statement('Job_id_query', fetch_result=True) -%}
            Select  md5(concat(to_varchar('{{var('batch_id')}}'),'-','{{this}}'))
    {%- endcall -%}
    {%- set  Job_id = load_result('Job_id_query') ['data'][0][0]  -%}
    {%- call statement('model_name', fetch_result=True) -%}
            Select  UPPER('{{this}}') as model_name
    {%- endcall -%}
    {%- set  model_name = load_result('model_name')['data'][0][0] -%}

    {%- call statement('table_name_query', fetch_result=True) -%}
            Select  UPPER(trim(split('{{model_name}}','.')[2],'"')) as DB_SH_TBL
    {%- endcall -%}
    {%- set  table_name = load_result('table_name_query')['data'][0][0] -%}



Select
 '{{model_name}}' as Model_Name,
 '{{table_name}}' AS Table_Name,
 '{{Job_id}}' AS JOB_ID,
 to_varchar('{{var('batch_id')}}') as Batch_Id, 
 PKID	,
CDMHOMESITEID	,
HOMESITEID	,
ADDRESSLINE1	,
CITY	,
STATE	,
ZIPCODE	,
LOTNUMBER	,
ISLOTEXCLUDED	,
COMMUNITYCODE	,
COMMUNITYNAME	,
DIVISIONCODE	,
DIVISIONNAME	,
REGIONCODE	,
REGIONNAME	,
COMMUNITYPHASENUMBER	,
PLANCODE	,
ISPLANEXCLUDED	,
ELEVATIONCODE	,
ARCHTYPECODE	,
TEMPLATENUMBER	,
HOMESTATUS	,
LOTSTATUSCODE	,
ISBROKERCOMISSION	,
ISHOMESITESOLD	,
ISHOMESITECLOSED	,
ISHOMESITECANCELLED	,
ISMODELHOMESITE	,
ISSHELLBUILDING	,
ISCREDITREPAIRSALES	,
ISCONTINGENTSALES	,
ISTRANSFER	,
ISSPECHOME	,
TRENCHDATE	,
ORIGINALDELIVERYDATE	,
ESTIMATEDDELIVERYDATE	,
ACTUALDELIVERYDATE	,
CANCELLATIONDATE	,
SALEDATE	
 from 
{{ source('Homesite', 'HOMESITEMASTER') }}

{% else %}
    select * from {{this}}  
{% endif %}