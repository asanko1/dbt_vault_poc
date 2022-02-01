{% macro Batch_insert_update(batch_status) %}

    {% if ''== 'INSERT' %}
        {% set query -%}
            Insert into ABC.PUBLIC.ABC_Batch_Details (Batch_ID,Batch_Name,Start_Timestamp,SourceSystem_Name,Batch_Status) 
            VALUES ('{{invocation_id}}','JDE',sysdate(),'JDE','In_Progress')
        {%- endset %}

        {% do run_query(query) %}

    {% elif '' == 'FAIL' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Batch_Details set END_TIMESTAMP = sysdate(), Batch_Status= 'Fail' where SourceSystem_Name = 'JDE' and 
            Batch_Status = 'In_Progress'  
        {%- endset %}

        {% do run_query(query) %}
        

    {% elif '' == 'SUCCESS' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Batch_Details set END_TIMESTAMP = sysdate(), Batch_Status= 'Success' where SourceSystem_Name = 'JDE' and 
            Batch_Status = 'In_Progress'  
        {%- endset %}

        {% do run_query(query) %}
        
    {% endif %}

{% endmacro %}