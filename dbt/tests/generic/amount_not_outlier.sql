{% test amount_not_outlier(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} > 100000

{% endtest %}
