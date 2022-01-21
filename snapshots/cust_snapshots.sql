{% snapshot cust_snapshot %}
    {{
        config(
        target_database='PC_DBT_DB',
        target_schema='snapshots',
        unique_key='CUSTOMER_PK',
        strategy='timestamp',
        updated_at='LOAD_DATE',
        
        )
    }}
        select * from {{ ref('sat_order_customer_details') }}
{% endsnapshot %}