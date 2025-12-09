
    
    



select first_transaction_ts
from "medallion"."main"."fct_customer_transactions"
where first_transaction_ts is null


