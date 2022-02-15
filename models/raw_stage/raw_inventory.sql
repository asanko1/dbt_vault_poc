-- The above depends_on statement needs to be added to the top of the model if ref is used in conditional. Please dont remove the above 

{{ 
    config(
        materialized='table',
        tags=["Source_system_inventory"]
       
        )
}}

-- The below state ment job_id_check is the query that checks if the job id that is created with the incoming batch is new or a rerun
-- if the output of the query returns 'True' that means it is a new batch and the model will run
-- if the output of the query is 'False' then model is in re run and further check is required

{%- call statement('Job_Id_check', fetch_result=True) -%}

-- i have not done a count * check with job_batch_detail table as if the snowflake does not return any  output the data[0][0] part in the set variable will fail as there is no output


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
	

    SELECT
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


{% else %}
-- this part is there in the code else there would be no ouput for the model so the create statement will fail
-- the below code will ensure that the table wil have the data as is 
    select * from {{this}}    
{% endif %}


