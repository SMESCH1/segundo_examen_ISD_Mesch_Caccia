

select transaction_count
from "medallion"."main"."fct_customer_transactions"
where transaction_count <= 0 or transaction_count is null

