
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select transaction_id
from "medallion"."main"."stg_transactions"
group by transaction_id
having count(*) > 1


  
  
      
    ) dbt_internal_test