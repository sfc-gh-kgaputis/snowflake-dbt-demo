{% macro get_partitions_to_replace(start_date_str, end_date_str, in_fmt='%Y-%m-%d', out_fmt='%Y-%m-%d') %}

    {% if not start_date_str or not end_date_str %}
        {{ exceptions.raise_compiler_error("Start and End dates are mandatory. Expected format: {Start Date}, {End Date}[, {In Format}, {Out Format}].") }}
    {% endif %}

    {% set result = dates_in_range(start_date_str, end_date_str, in_fmt=in_fmt, out_fmt=out_fmt) %}

    {{ return({'start_date': start_date_str, 'end_date': end_date_str, 'result': result}) }}
    
{% endmacro %}

{% macro dates_in_range(start_date_str, end_date_str=none, in_fmt="%Y-%m-%d", out_fmt="%Y-%m-%d") %}

    {% set end_date_str = start_date_str if end_date_str is none else end_date_str %}

    {% set start_date = convert_datetime(start_date_str, in_fmt) %}
    {% set end_date = convert_datetime(end_date_str, in_fmt) %}

    {% set day_count = (end_date - start_date).days %}
    {% if day_count < 0 %}
        {% set msg -%}
            Partiton start date is after the end date ({{ start_date }}, {{ end_date }})
        {%- endset %}

        {{ exceptions.raise_compiler_error(msg, model) }}
    {% endif %}

    {% set date_list = [] %}
    {% for i in range(0, day_count + 1) %}
        {% set the_date = (modules.datetime.timedelta(days=i) + start_date) %}
        {% if not out_fmt %}
            {% set _ = date_list.append("'" + the_date|string + "'") %}
        {% else %}
            {% set _ = date_list.append("'" + the_date.strftime(out_fmt)|string + "'") %}
        {% endif %}
    {% endfor %}

    {{ return(date_list) }}
{% endmacro %}

{% macro convert_datetime(date_str, date_fmt) %}

  {% set error_msg -%}
      The provided partition date '{{ date_str }}' does not match the expected format '{{ date_fmt }}'
  {%- endset %}

  {% set res = try_or_compiler_error(error_msg, modules.datetime.datetime.strptime, date_str.strip(), date_fmt) %}
  {{ return(res) }}

{% endmacro %}
