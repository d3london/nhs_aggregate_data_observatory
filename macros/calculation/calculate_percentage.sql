{% macro calculate_percentage(numerator, denominator) %}

    {% if denominator and denominator != 0 %} -- avoid divide by 0 error
        round(({{ numerator }}::float / {{ denominator }}::float) * 100.0, 2)
    {% else %}
        null
    {% endif %}

{% endmacro %}