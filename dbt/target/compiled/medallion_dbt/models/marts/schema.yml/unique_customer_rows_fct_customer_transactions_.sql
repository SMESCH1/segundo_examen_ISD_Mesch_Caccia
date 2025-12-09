

select customer_id
from "medallion"."main"."fct_customer_transactions"
group by customer_id
having count(*) > 1

