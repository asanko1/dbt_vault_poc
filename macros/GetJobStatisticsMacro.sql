{% macro GetJobStatisticMacro() %}
    {%- set current_model_full_name = '{{this}}'.split('.') -%}
    {%- set current_table_name =current_model_full_name[-1] -%}
    {%- call statement('GetMetrics', fetch_result=True) -%}
        Select SourceTableName,    SourceAttributeNames,
        OperationName,JobMetricsMaster_id from ABC.Public.JobMetricsMaster
        WHERE SourceTableName = '{{current_table_name}}' 
    {%- endcall -%}
    {% if execute %}
        {%- set get_metrics_frame= load_result('GetMetrics') -%}
        {%- set no_of_metrics= get_metrics_frame['response']['rows_affected'] | int -%}
        {%- set get_metrics_array= load_result('GetMetrics')['data'] -%}
        {% set query_metric_entries %}
            {% for i in range(no_of_metrics) %}
                Insert into ABC.Public.ABC_JOBBALANCE(JOB_ID,Batch_ID,Metric_ID,SourceValue)
                Select  concat('{{running_job_id}}','-','{{this}}'),'{{batch_id}}',count(distinct {{get_metrics_array[i][1]}}) , {{get_metrics_array[i][3]}}
                from ShreyDBTPOC.Demo_dev.{{get_metrics_array[i][0]}}
            {% endfor %}
        {% endset %}

    {% endif %}
    {{query_metric_entries}}
{% endmacro %}