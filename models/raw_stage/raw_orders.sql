{{ config(
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

{{ Job_insert_update('INSERT','{{this}}', Job_id,var('batch_id')) }}

SELECT
    '{{model_name}}' as Model_Name,
    '{{table_name}}' AS Table_Name,
    '{{Job_id}}' AS JOB_ID,
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

{{run_end_hook(Job_id,model_name,table_name)}}
{{GetJobStatisticMacro(Job_id,table_name)}}