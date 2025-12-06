
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_amount_all
from "medallion"."main"."fct_customer_transactions"
where total_amount_all is null



  
  
      
    ) dbt_internal_test