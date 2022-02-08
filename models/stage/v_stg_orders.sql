{{ config(
    tags=["Source_system_1"]
) }}

{%- set yaml_metadata -%}
source_model: 'raw_orders'
derived_columns:
  NATION_KEY: 'CUSTOMER_NATION_KEY'
  REGION_KEY: 'CUSTOMER_REGION_KEY'
  RECORD_SOURCE: '!TPCH-ORDERS'
  EFFECTIVE_FROM: 'ORDERDATE'
hashed_columns:
  CUSTOMER_PK: 'CUSTOMERKEY'
  LINK_CUSTOMER_NATION_PK:
    - 'CUSTOMERKEY'
    - 'CUSTOMER_NATION_KEY'
  CUSTOMER_NATION_PK: 'CUSTOMER_NATION_KEY'
  CUSTOMER_REGION_PK: 'CUSTOMER_REGION_KEY'
  NATION_PK: 'CUSTOMER_NATION_KEY'
  REGION_PK: 'CUSTOMER_REGION_KEY'
  NATION_REGION_PK:
    - 'CUSTOMER_NATION_KEY'
    - 'CUSTOMER_REGION_KEY'
  ORDER_PK: 'ORDERKEY'
  ORDER_CUSTOMER_PK:
    - 'CUSTOMERKEY'
    - 'ORDERKEY'
  LINEITEM_PK:
    - 'ORDERKEY'
    - 'LINENUMBER'
  LINK_LINEITEM_ORDER_PK:
    - 'ORDERKEY'
    - 'ORDERKEY'
    - 'LINENUMBER'
  PART_PK: 'PARTKEY'
  SUPPLIER_PK: 'SUPPLIERKEY'
  INVENTORY_PK:
    - 'PARTKEY'
    - 'SUPPLIERKEY'
  INVENTORY_ALLOCATION_PK:
    - 'LINENUMBER'
    - 'PARTKEY'
    - 'SUPPLIERKEY'
  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'CUSTOMERKEY'
      - 'CUSTOMER_NAME'
      - 'CUSTOMER_ADDRESS'
      - 'CUSTOMER_PHONE'
      - 'CUSTOMER_ACCBAL'
      - 'CUSTOMER_MKTSEGMENT'
      - 'CUSTOMER_COMMENT'
  CUSTOMER_NATION_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'CUSTOMER_NATION_NAME'
      - 'CUSTOMER_NATION_COMMENT'
      - 'CUSTOMER_NATION_KEY'
  CUSTOMER_REGION_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'CUSTOMER_REGION_NAME'
      - 'CUSTOMER_REGION_COMMENT'
      - 'CUSTOMER_REGION_KEY'
  LINEITEM_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'ORDERKEY'
      - 'LINENUMBER'
      - 'COMMITDATE'
      - 'DISCOUNT'
      - 'EXTENDEDPRICE'
      - 'LINESTATUS'
      - 'LINE_COMMENT'
      - 'QUANTITY'
      - 'RECEIPTDATE'
      - 'RETURNFLAG'
      - 'SHIPDATE'
      - 'SHIPINSTRUCT'
      - 'SHIPMODE'
      - 'TAX'
  ORDER_HASHDIFF:
    is_hashdiff: true
    columns:
      - 'ORDERKEY'
      - 'CLERK'
      - 'ORDERDATE'
      - 'ORDERPRIORITY'
      - 'ORDERSTATUS'
      - 'ORDER_COMMENT'
      - 'SHIPPRIORITY'
      - 'TOTALPRICE'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{% set source_model = metadata_dict['source_model'] %}

{% set derived_columns = metadata_dict['derived_columns'] %}

{% set hashed_columns = metadata_dict['hashed_columns'] %}

WITH staging AS (
{{ dbtvault.stage(include_source_columns=true,
                  source_model=source_model,
                  derived_columns=derived_columns,
                  hashed_columns=hashed_columns,
                  ranked_columns=none) }}
)

SELECT *,
       TO_DATE('{{ var('load_date') }}') AS LOAD_DATE
FROM staging