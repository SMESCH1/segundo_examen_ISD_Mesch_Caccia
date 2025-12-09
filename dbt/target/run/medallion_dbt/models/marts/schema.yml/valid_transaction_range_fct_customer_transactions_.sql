
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "medallion"."main"."fct_customer_transactions"
where first_transaction_ts > last_transaction_ts


  
  
      
    ) dbt_internal_test