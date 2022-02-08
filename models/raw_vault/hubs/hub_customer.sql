{{ config(
    tags=["Source_system_1"]
) }}

{%- set source_model = "v_stg_orders" -%}
{%- set src_pk = "CUSTOMER_PK" -%}
{%- set src_nk = "CUSTOMERKEY" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = ["RECORD_SOURCE","BATCH_ID","JOB_ID"] -%}

{{ dbtvault.hub(src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
                src_source=src_source, source_model=source_model) }}