{% test no_duplicate_rows(model) %}

select *
from {{ model }}
group by *
having count(*) > 1

{% endtest %}
