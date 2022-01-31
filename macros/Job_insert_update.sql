{% macro Job_insert_update(running_job_status,running_job_name) %}

    {% if ('{{job_status}}'.upper()) == 'INSERT' %}
        {% set query -%}
            Insert into ABC.PUBLIC.ABC_Job_Details(Job_ID,JobName,Start_Timestamp,Job_Status) 
            VALUES ('{{invocation_id}}',running_job_name,sysdate(),'In_Progress')
        {%- endset %}

        {% do run_query(query) %}

    {% elif ('{{job_status}}'.upper()) == 'FAIL' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Job_Details
	        SET End_Timestamp = sysdate(),Job_Status = 'FAIL' where job_name =running_job_name and job_status ='In_Progress'  ;
        {%- endset %}

        {% do run_query(query) %}
        
    {% elif ('{{job_status}}'.upper()) == 'SUCCESS' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Job_Details
	        SET End_Timestamp = sysdate(),Job_Status = 'SUCCESS' where job_name =running_job_name and job_status ='In_Progress'  ;
        {%- endset %}

        {% do run_query(query) %}
        
    {% endif %}

{% endmacro %}