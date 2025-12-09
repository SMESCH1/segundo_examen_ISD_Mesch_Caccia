

select *
from "medallion"."main"."fct_customer_transactions"
where first_transaction_ts > last_transaction_ts

