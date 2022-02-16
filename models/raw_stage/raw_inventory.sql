-- The above depends_on statement needs to be added to the top of the model if ref is used in conditional. Please dont remove the above 

{{ 
    config(
        materialized='table',
        tags=["Source_system_orders"]
       
        )
}}

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


    SELECT
        '{{this}}' as this_model,
        sysdate() as  System_date,
        '{{model_name}}' as Model_Name,
        '{{table_name}}' AS Table_Name,
        '{{Job_id}}' AS JOB_ID,
        to_varchar('{{var('batch_id')}}') as Batch_Id, 
        a.PS_PARTKEY AS PARTKEY,
        a.PS_SUPPKEY AS SUPPLIERKEY,
        a.PS_AVAILQTY AS AVAILQTY,
        a.PS_SUPPLYCOST AS SUPPLYCOST,
        a.PS_COMMENT AS PART_SUPPLY_COMMENT,
        b.S_NAME AS SUPPLIER_NAME,
        b.S_ADDRESS AS SUPPLIER_ADDRESS,
        b.S_NATIONKEY AS SUPPLIER_NATION_KEY,
        b.S_PHONE AS SUPPLIER_PHONE,
        b.S_ACCTBAL AS SUPPLIER_ACCTBAL,
        b.S_COMMENT AS SUPPLIER_COMMENT,
        c.P_NAME AS PART_NAME,
        c.P_MFGR AS PART_MFGR,
        c.P_BRAND AS PART_BRAND,
        c.P_TYPE AS PART_TYPE,
        c.P_SIZE AS PART_SIZE,
        c.P_CONTAINER AS PART_CONTAINER,
        c.P_RETAILPRICE AS PART_RETAILPRICE,
        c.P_COMMENT AS PART_COMMENT,
        d.N_NAME AS SUPPLIER_NATION_NAME,
        d.N_COMMENT AS SUPPLIER_NATION_COMMENT,
        d.N_REGIONKEY AS SUPPLIER_REGION_KEY,
        e.R_NAME AS SUPPLIER_REGION_NAME,
        e.R_COMMENT AS SUPPLIER_REGION_COMMENT

    FROM {{ source('tpch_sample', 'PARTSUPP') }} AS a
    LEFT JOIN {{ source('tpch_sample', 'SUPPLIER') }} AS b
        ON a.PS_SUPPKEY = b.S_SUPPKEY
    LEFT JOIN {{ source('tpch_sample', 'PART') }} AS c
        ON a.PS_PARTKEY = c.P_PARTKEY
    LEFT JOIN {{ source('tpch_sample', 'NATION') }} AS d    
        ON b.S_NATIONKEY = d.N_NATIONKEY
    LEFT JOIN {{ source('tpch_sample', 'REGION') }} AS e
        ON d.N_REGIONKEY = e.R_REGIONKEY
 AND UPPER('{{Last_Job_Status}}')<>'SUCCESS'
{% else %}
-- this part is there in the code else there would be no ouput for the model so the create statement will fail
-- the below code will ensure that the table wil have the data as is 
    select * from {{this}} 

{% endif %}