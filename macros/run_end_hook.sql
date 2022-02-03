{% macro run_end_hook(job_id,table_name) %}

    {% if execute %}
        {%- call statement('delta_count', fetch_result=True) -%}
            SELECT count(*) FROM {{table_name}} where JOBID = concat(to_varchar('{{var('job_id')}}'),'-','inventory')
        {%- endcall -%}

        {%- set Query_count = load_result('delta_count')  -%}
        {%- set out_result = Query_count['data'][0][0] -%}

        {% if out_result > 0 %}
            {{ Job_insert_update('SUCCESS','{{table_name}}','pipeline_id','batch_id','inventory') }}
        {% else %}
            {{ Job_insert_update('FAIL','{{table_name}}','pipeline_id','batch_id','inventory') }}
        {% endif %}
    {% endif %}
{% endmacro %}