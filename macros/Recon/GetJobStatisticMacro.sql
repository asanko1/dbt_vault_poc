{% macro get_query_result() %}    
    {%- call statement('GetMetrics', fetch_result=True) -%}
        Select SourceTableName,    SourceAttributeNames,
        OperationName,JobMetricsMaster_id from ABC.Public.JobMetricsMaster
        WHERE SourceTableName = 'RAW_ORDERS' 
    {%- endcall -%}
    {%- set getmetrics= load_result('GetMetrics') -%}
    {%- set count1= load_result('GetMetrics')['response']['rows_affected'] -%}
    {%- set check1= load_result('GetMetrics')['data'][0][3] -%}
    {{return (getmetrics)}}
    
{% endmacro %}

print {{getmetrics}}

   