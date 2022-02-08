{% macro run_end_hook(job_id,model_name,table_name) %}

    
        {%- call statement('delta_count', fetch_result=True) -%}
            SELECT count(*) FROM {{model_name}} where JOB_ID = to_varchar('{{job_id}}')
        {%- endcall -%}
        {% set query -%}
            Insert into PC_DBT_DB.DBT_ABASAK_CUST_DETAIL.job_id(Job_ID) 
            VALUES ('{{job_id}}') ;
        {%- endset %}

        {% do run_query(query) %}
        
        {% if execute %}    
            {%- set Query_count = load_result('delta_count')['data'][0][0] | int -%}
            {%- set out_result = Query_count | int -%}
           
            
            {% if out_result > 0 %} 
                {{ Job_insert_update('SUCCESS','{{table_name}}',job_id,'batch_id') }}
            {% else %}
                {{ Job_insert_update('FAIL','{{table_name}}',job_id,'batch_id') }}
            {% endif %}
        {% endif %}
{% endmacro %}