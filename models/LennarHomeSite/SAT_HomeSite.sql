{{ config(
    materialized='table',
    tags=["SAT_HomeSite"]
) }}

---definitions
--- Batch_Id is the one that is provided to dbt by ADF
--- Model_Name is the name of the fully qualified name : <Database_name>.<Schema_name>.<table_name>. Equivalent of dbt {{this}}
--- Job_Id is the one that we generate by MD5(BatchId+ModelName)
--- Table_Name is just the table name without any database and schema names


{%- call statement('model_name', fetch_result=True) -%}
        Select  LOWER('{{this}}') as model_name
{%- endcall -%}
{%- set  model_name = load_result('model_name')['data'][0][0] -%}

{%- call statement('table_name_query', fetch_result=True) -%}
        Select  LOWER(trim(split('{{this}}','.')[2],'"')) as DB_SH_TBL
{%- endcall -%}
{%- set  table_name = load_result('table_name_query')['data'][0][0] -%}


{%- call statement('Job_id_query', fetch_result=True) -%}
        Select  md5(concat(to_varchar('{{var('batch_id')}}'),'-','{{table_name}}'))
{%- endcall -%}
{%- set  Job_id = load_result('Job_id_query') ['data'][0][0]  -%}

--Last Job Status check
{%- call statement('Last_Job_Status_check', fetch_result=True) -%}
    --return the lastest job id or 'Never_Run' if it is the first time
    SELECT job_status
    FROM   (SELECT job_status,
                Rank()
                    OVER (
                    ORDER BY start_timestamp DESC) AS OrderOfExecution
            FROM   (SELECT job_status,
                        start_timestamp
                    FROM   abc.PUBLIC.abc_job_details
                    WHERE  job_id = '{{Job_id}}'
                    UNION
                    SELECT 'Never_Run'  AS job_Status,
                        '1900-01-01' AS Start_TimeStamp))
    WHERE  orderofexecution = 1 
{%- endcall -%}  

{%  set Last_Job_Status =load_result('Last_Job_Status_check') ['data'][0][0] %}
{% set query -%}
            Insert into PC_DBT_DB.DBT_ABASAK_CUST_DETAIL.job_id(Job_ID) 
            VALUES ('{{Last_Job_Status}}') ;
{%- endset %}
{% do run_query(query) %}
{% if Last_Job_Status != 'SUCCESS' %}
    {{ Job_insert_update('INSERT','{{table_name}}', Job_id,var('batch_id')) }}



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
{{ GetJobStatisticMacro(Job_id,'Sat_HomeSite') }}

{% else %}
-- this part is there in the code else there would be no ouput for the model so the create statement will fail
-- the below code will ensure that the table wil have the data as is 
    select * from {{this}} 

 {{ GetJobStatisticMacro(Job_id,'Sat_HomeSite') }}
{% endif %}