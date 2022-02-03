{% macro Job_insert_update(running_job_status,running_job_name,running_job_id,b_id,model_name) %}

    {% if running_job_status == 'INSERT' %}
        {% set query -%}
            Insert into ABC.PUBLIC.ABC_Job_Details(Job_ID,JobName,Start_Timestamp,Job_Status,BATCH_ID) 
            VALUES (concat('{{running_job_id}}','-','{{model_name}}'),'{{running_job_name}}',sysdate(),'In_Progress','{{b_id}}')
        {%- endset %}

        {% do run_query(query) %}

    {% elif running_job_status== 'FAIL' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Job_Details
	        SET End_Timestamp = sysdate(),Job_Status = 'FAIL' where Job_ID = concat('{{running_job_id}}','-','{{model_name}}')  ;
        {%- endset %}

        {% do run_query(query) %}
        
    {% elif running_job_status == 'SUCCESS' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Job_Details
	        SET End_Timestamp = sysdate(),Job_Status = 'SUCCESS' where Job_ID = concat('{{running_job_id}}','-','{{model_name}}')  ;
        {%- endset %}

        {% do run_query(query) %}
        
    {% endif %}

{% endmacro %}