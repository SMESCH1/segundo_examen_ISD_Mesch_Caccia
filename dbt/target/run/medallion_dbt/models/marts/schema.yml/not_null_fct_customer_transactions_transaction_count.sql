
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_count
from "medallion"."main"."fct_customer_transactions"
where transaction_count is null



  
  
      
    ) dbt_internal_test