{% macro Job_insert_update(running_job_status,job_name,running_job_id,b_id) %}

    {% if running_job_status == 'INSERT' %}
        {% set query -%}
            Insert into ABC.PUBLIC.ABC_Job_Details(Job_ID,JobName,Start_Timestamp,Job_Status,BATCH_ID) 
            VALUES ('{{running_job_id}}','{{this}}',sysdate(),'In_Progress','{{b_id}}')
        {%- endset %}

        {% do run_query(query) %}

    {% elif running_job_status== 'FAIL' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Job_Details
	        SET End_Timestamp = sysdate(),Job_Status = 'FAIL' where Job_ID = '{{running_job_id}}'  ;
        {%- endset %}

        {% do run_query(query) %}
        
    {% elif running_job_status == 'SUCCESS' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Job_Details
	        SET End_Timestamp = sysdate(),Job_Status = 'SUCCESS' where  Job_ID = '{{running_job_id}}'  ;
        {%- endset %}

        {% do run_query(query) %}

        {% set query -%}
            Insert into PC_DBT_DB.DBT_ABASAK_CUST_DETAIL.job_id(Job_ID) 
            VALUES ('{{running_job_id}}') ;
        {%- endset %}

        {% do run_query(query) %}
        
    {% endif %}

{% endmacro %}