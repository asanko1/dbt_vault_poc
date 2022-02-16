{{ config(
    materialized='table',
    tags=["Source_system_orders"]
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


SELECT
    '{{this}}' as this_model,
     UPPER('{{Last_Job_Status}}') as value1,
    'new_run_3' as temp,
    '{{model_name}}' as Model_Name,
    '{{table_name}}' AS Table_Name,
    '{{Job_id}}' AS JOB_ID,
     current_timestamp()  as timestamp,
    to_varchar('{{var('batch_id')}}') as Batch_Id,
    a.L_ORDERKEY AS ORDERKEY,
    a.L_PARTKEY AS PARTKEY ,
    a.L_SUPPKEY AS SUPPLIERKEY,
    a.L_LINENUMBER AS LINENUMBER,
    a.L_QUANTITY AS QUANTITY,
    a.L_EXTENDEDPRICE AS EXTENDEDPRICE,
    a.L_DISCOUNT AS DISCOUNT,
    a.L_TAX AS TAX,
    a.L_RETURNFLAG AS RETURNFLAG,
    a.L_LINESTATUS AS LINESTATUS,
    a.L_SHIPDATE AS SHIPDATE,
    a.L_COMMITDATE AS COMMITDATE,
    a.L_RECEIPTDATE AS RECEIPTDATE,
    a.L_SHIPINSTRUCT AS SHIPINSTRUCT,
    a.L_SHIPMODE AS SHIPMODE,
    a.L_COMMENT AS LINE_COMMENT,
    b.O_CUSTKEY AS CUSTOMERKEY,
    b.O_ORDERSTATUS AS ORDERSTATUS,
    b.O_TOTALPRICE AS TOTALPRICE,
    b.O_ORDERDATE AS ORDERDATE,
    b.O_ORDERPRIORITY AS ORDERPRIORITY,
    b.O_CLERK AS CLERK,
    b.O_SHIPPRIORITY AS SHIPPRIORITY,
    b.O_COMMENT AS ORDER_COMMENT,
    c.C_NAME AS CUSTOMER_NAME,
    c.C_ADDRESS AS CUSTOMER_ADDRESS,
    c.C_NATIONKEY AS CUSTOMER_NATION_KEY,
    c.C_PHONE AS CUSTOMER_PHONE,
    c.C_ACCTBAL AS CUSTOMER_ACCBAL,
    c.C_MKTSEGMENT AS CUSTOMER_MKTSEGMENT,
    c.C_COMMENT AS CUSTOMER_COMMENT,
    d.N_NAME AS CUSTOMER_NATION_NAME,
    d.N_REGIONKEY AS CUSTOMER_REGION_KEY,
    d.N_COMMENT AS CUSTOMER_NATION_COMMENT,
    e.R_NAME AS CUSTOMER_REGION_NAME,
    e.R_COMMENT AS CUSTOMER_REGION_COMMENT

FROM {{ source('tpch_sample', 'ORDERS') }} AS b
LEFT JOIN {{ source('tpch_sample', 'LINEITEM') }} AS a
    ON a.L_ORDERKEY = b.O_ORDERKEY
LEFT JOIN {{ source('tpch_sample', 'CUSTOMER') }} AS c
    ON b.O_CUSTKEY  = c.C_CUSTKEY
LEFT JOIN {{ source('tpch_sample', 'NATION') }} AS d
    ON c.C_NATIONKEY  = d.N_NATIONKEY
LEFT JOIN {{ source('tpch_sample', 'REGION') }} AS e
    ON d.N_REGIONKEY  = e.R_REGIONKEY
LEFT JOIN {{ source('tpch_sample', 'PART') }} AS g
    ON a.L_PARTKEY = g.P_PARTKEY
LEFT JOIN {{ source('tpch_sample', 'SUPPLIER') }} AS h
    ON a.L_SUPPKEY = h.S_SUPPKEY
LEFT JOIN {{ source('tpch_sample', 'NATION') }} AS j
    ON h.S_NATIONKEY = j.N_NATIONKEY
LEFT JOIN {{ source('tpch_sample', 'REGION') }} AS k
    ON j.N_REGIONKEY = k.R_REGIONKEY
WHERE b.O_ORDERDATE = TO_DATE('{{ var('load_date') }}')
--Skip the model in case it was successful earlier for the same batch
AND UPPER('{{Last_Job_Status}}')<>'SUCCESS'

 {{ GetJobStatisticMacro(Job_id,'RAW_ORDER') }}

{% else %}
-- this part is there in the code else there would be no ouput for the model so the create statement will fail
-- the below code will ensure that the table wil have the data as is 
    select * from {{this}} 

 {{ GetJobStatisticMacro(Job_id,'RAW_ORDER') }}
{% endif %}