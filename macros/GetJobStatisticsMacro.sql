{% macro GetJobStatisticMacro(Job_id,table_name) %}
    {%- call statement('GetMetrics', fetch_result=True) -%}
        Select SourceTableName,    SourceAttributeNames,
        OperationName,JobMetricsMaster_id from ABC.Public.JobMetricsMaster
        WHERE SourceTableName = '{{table_name}}' 
    {%- endcall -%}
    {% if execute %}
        {%- set get_metrics_frame= load_result('GetMetrics') -%}
        {%- set no_of_metrics= get_metrics_frame['response']['rows_affected'] | int -%}
        {%- set get_metrics_array= load_result('GetMetrics')['data'] -%}
        
        {% for i in range(no_of_metrics) %}
            {%- set query_metric_entries -%}
                Insert into ABC.Public.ABC_JOBBALANCE(JOB_ID,Batch_ID,Metric_ID,SourceValue)
                Select '{{Job_id}}','{{batch_id}}',count(distinct {{get_metrics_array[i][1]}}) , {{get_metrics_array[i][3]}}
                from ShreyDBTPOC.Demo_dev.{{get_metrics_array[i][0]}};
            {%- endset -%}
            {% do run_query(query_metric_entries) %}
        {% endfor %}
    {% endif %}     
{% endmacro %}