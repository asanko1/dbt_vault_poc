
{{ 
    config(
        materialized='table',
         tags=["Source_system_1"]
        )
}}

{%- call statement('model_id_query', fetch_result=True) -%}
        Select  md5(concat(to_varchar('{{var('job_id')}}'),'-','{{this}}'))
{%- endcall -%}
{%- set  model_id = load_result('model_id_query') ['data'][0][0]  -%}
{{ Job_insert_update('INSERT','{{this}}', model_id,var('batch_id')) }}


SELECT
    '{{this}}' as test,
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
    e.R_COMMENT AS SUPPLIER_REGION_COMMENT,
    to_varchar('{{var('batch_id')}}') as BATCH_ID,
    md5(concat(to_varchar('{{var('job_id')}}'),'-','{{this}}')) as JOBID

FROM {{ source('tpch_sample', 'PARTSUPP') }} AS a
LEFT JOIN {{ source('tpch_sample', 'SUPPLIER') }} AS b
    ON a.PS_SUPPKEY = b.S_SUPPKEY
LEFT JOIN {{ source('tpch_sample', 'PART') }} AS c
    ON a.PS_PARTKEY = c.P_PARTKEY
LEFT JOIN {{ source('tpch_sample', 'NATION') }} AS d    
    ON b.S_NATIONKEY = d.N_NATIONKEY
LEFT JOIN {{ source('tpch_sample', 'REGION') }} AS e
    ON d.N_REGIONKEY = e.R_REGIONKEY
JOIN {{ ref('raw_orders') }} AS f
    ON a.PS_PARTKEY = f.PARTKEY AND a.PS_SUPPKEY=f.SUPPLIERKEY
ORDER BY a.PS_PARTKEY, a.PS_SUPPKEY

	{{run_end_hook(model_id,'PC_DBT_DB.DBT_ABASAK_CUST_DETAIL.RAW_INVENTORY','inventory')}}


