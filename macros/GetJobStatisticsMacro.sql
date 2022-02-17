{% macro GetJobStatisticMacro(Job_id,table_name) %}
    {%- call statement('GetMetrics', fetch_result=True) -%}
        Select SourceTableName,    SourceAttributeNames,
        OperationName,JobMetricsMaster_id, 'Source' as StatsType from ABC.Public.JobMetricsMaster
        WHERE SourceTableName = '{{table_name}}' 
        UNION All
        Select TargetTableName,    TargetAttributeNames,
        OperationName,JobMetricsMaster_id, 'Target' as StatsType from ABC.Public.JobMetricsMaster
        WHERE TargetTableName = '{{table_name}}' 
    {%- endcall -%}
    {% if execute %}
        {%- set get_metrics_frame= load_result('GetMetrics') -%}
        {%- set no_of_metrics= get_metrics_frame['response']['rows_affected'] | int -%}
        {%- set get_metrics_array= load_result('GetMetrics')['data'] -%}
        
        {% for i in range(no_of_metrics) %}
            {%- set query_metric_entries -%}
                {% if get_metrics_array[i][4] == 'Source' %}
                    Insert into ABC.Public.ABC_JOBBALANCE(JOB_ID,Batch_ID,SourceValue,Metric_ID)
                    Select '{{Job_id}}','{{var('batch_id')}}',count(distinct {{get_metrics_array[i][1]}}) , {{get_metrics_array[i][3]}}
                    from PC_DBT_DB.DBT_ABASAK.{{get_metrics_array[i][0]}};
                {% else %}
                    update ABC.Public.ABC_JOBBALANCE
                    SET TargetValue = (Select count(distinct {{get_metrics_array[i][1]}}) from PC_DBT_DB.DBT_ABASAK.{{get_metrics_array[i][0]}})
                    WHERE JOB_ID='{{Job_id}}' AND 
                    Batch_ID='{{var('batch_id')}}'
                    AND Metric_ID='{{get_metrics_array[i][3]}}';
                    UPDATE ABC.Public.ABC_JOBBALANCE
                    SET METRIC_RESULT  = (Select ABS(TO_NUMBER(SOURCEVALUE)-TO_NUMBER(TargetValue))
                    from ABC.Public.ABC_JOBBALANCE where JOB_ID='{{Job_id}}' AND 
                    Batch_ID='{{var('batch_id')}}'
                    AND Metric_ID='{{get_metrics_array[i][3]}}') where 
                    JOB_ID='{{Job_id}}' AND 
                    Batch_ID='{{var('batch_id')}}'
                    AND Metric_ID='{{get_metrics_array[i][3]}}';
                {% endif %}
            {%- endset -%}
            {% do run_query(query_metric_entries) %}
        {% endfor %}
    {% endif %}     
{% endmacro %}
