{{ config(
    materialized='table',
    tags=["Source_system_test_bracket"]
) }}

select * from {{this}}