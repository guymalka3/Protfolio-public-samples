{% macro sum_total_events(column_name, events= []) %}

  {% for event_name in events %}
    SUM(CASE WHEN {{column_name}} = '{{event_name}}' THEN 1 END) AS t_{{event_name}}

      {%- if not loop.last %},{% endif -%}
 {% endfor %}

{% endmacro %}