{% test unique_customer_rows(model) %}

select customer_id
from {{ model }}
group by customer_id
having count(*) > 1

{% endtest %}
