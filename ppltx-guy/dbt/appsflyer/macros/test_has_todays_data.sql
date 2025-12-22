{% test has_todays_data(model, date_column='event_time') %}
    select
        {% if should_store_failures() %}
            *
        {% else %}
            count(case when DATE({{ date_column }}) != CURRENT_DATE() then 1 end) as failures
        {% endif %}
    from {{ model }}
    having count(case when DATE({{ date_column }}) = CURRENT_DATE() then 1 end) = 0
{% endtest %}
