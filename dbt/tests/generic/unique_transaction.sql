{% test unique_transaction_id(model, column_name) %}

select {{ column_name }}
from {{ model }}
group by {{ column_name }}
having count(*) > 1

{% endtest %}
