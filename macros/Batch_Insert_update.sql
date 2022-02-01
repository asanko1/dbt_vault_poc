{% macro Batch_insert_update(b_status,b_id,SourceSystem_Name) %}

    {%- if b_status == 'INSERT' -%}
        {% set query -%}
            Insert into ABC.PUBLIC.ABC_Batch_Details (Batch_ID,Batch_Name,Start_Timestamp,SourceSystem_Name,Batch_Status) 
            VALUES ('{{b_id}}','JDE',sysdate(),'{{SourceSystem_Name}}','In_Progress')
        {%- endset %}

        {% do run_query(query) %}

    {% elif b_status == 'FAIL' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Batch_Details set END_TIMESTAMP = sysdate(), Batch_Status= 'Fail' where SourceSystem_Name = '{{SourceSystem_Name}}' and 
            Batch_Status = 'In_Progress'  
        {%- endset %}

        {% do run_query(query) %}
        

    {% elif b_status == 'SUCCESS' %}

        {% set query -%}
            Update ABC.PUBLIC.ABC_Batch_Details set END_TIMESTAMP = sysdate(), Batch_Status= 'Success' where SourceSystem_Name = '{{SourceSystem_Name}}' and 
            Batch_Status = 'In_Progress'  
        {%- endset %}

        {% do run_query(query) %}
        
    {% endif %}

{% endmacro %}