{{ config(
    materialized='table',
    tags=["Source_system_1"]
) }}

---definitions
--- Batch_Id is the one that is provided to dbt by ADF
--- Model_Name is the name of the fully qualified name : <Database_name>.<Schema_name>.<table_name>. Equivalent of dbt {{this}}
--- Job_Id is the one that we generate by MD5(BatchId+ModelName)
--- Table_Name is just the table name without any database and schema names
{%- call statement('Job_id_query', fetch_result=True) -%}
        Select  md5(concat(to_varchar('{{var('batch_id')}}'),'-','{{this}}'))
{%- endcall -%}
{%- set  Job_id = load_result('Job_id_query') ['data'][0][0]  -%}

{%- call statement('model_name', fetch_result=True) -%}
        Select  UPPER('{{this}}') as model_name
{%- endcall -%}
{%- set  model_name = load_result('model_name')['data'][0][0] -%}

{%- call statement('table_name_query', fetch_result=True) -%}
        Select  UPPER(trim(split('{{this}}','.')[2],'"')) as DB_SH_TBL
{%- endcall -%}
{%- set  table_name = load_result('table_name_query')['data'][0][0] -%}

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

{% if Last_Job_Status != 'Success' %}
    {{ Job_insert_update('INSERT','{{this}}', Job_id,var('batch_id')) }}
{% endif %}



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

{{run_end_hook(Job_id,model_name,table_name)}}
{{GetJobStatisticMacro(Job_id,table_name)}}