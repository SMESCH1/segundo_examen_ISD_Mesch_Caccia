
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select first_transaction_ts
from "medallion"."main"."fct_customer_transactions"
where first_transaction_ts is null



  
  
      
    ) dbt_internal_test