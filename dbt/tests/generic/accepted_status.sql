{% test accepted_status(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} not in ('completed', 'pending', 'failed')

{% endtest %}
