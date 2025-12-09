{% test customer_exists_in_silver(model, column_name) %}

with silver as (
    select distinct customer_id
    from {{ ref('stg_transactions') }}
)
select g.{{ column_name }}
from {{ model }} g
left join silver s
    on g.{{ column_name }} = s.customer_id
where s.customer_id is null

{% endtest %}
