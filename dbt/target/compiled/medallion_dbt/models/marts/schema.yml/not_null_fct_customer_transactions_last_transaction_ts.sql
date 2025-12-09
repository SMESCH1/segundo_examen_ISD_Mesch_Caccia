
    
    



select last_transaction_ts
from "medallion"."main"."fct_customer_transactions"
where last_transaction_ts is null


