{% macro Job_insert_update(running_job_status,running_job_name,running_job_id) %}

    {% if running_job_status == 'INSERT' %}
        {% set query -%}
            Insert into ABC.PUBLIC.ABC_Job_Details(Job_ID,JobName,Start_Timestamp,Job_Status) 
            VALUES (running_job_id,running_job_name,sysdate(),'In_Progress')
        {%- endset %}

        {% do run_query(query) %}

    {% elif running_job_status== 'FAIL' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Job_Details
	        SET End_Timestamp = sysdate(),Job_Status = 'FAIL' where job_name =running_job_name and job_status ='In_Progress'  ;
        {%- endset %}

        {% do run_query(query) %}
        
    {% elif running_job_status == 'SUCCESS' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Job_Details
	        SET End_Timestamp = sysdate(),Job_Status = 'SUCCESS' where job_name =running_job_name and job_status ='In_Progress'  ;
        {%- endset %}

        {% do run_query(query) %}
        
    {% endif %}

{% endmacro %}