
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select transaction_ts
from "medallion"."main"."stg_transactions"
where transaction_ts is null



  
  
      
    ) dbt_internal_test