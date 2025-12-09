
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select status
from "medallion"."main"."stg_transactions"
where status not in ('completed', 'pending', 'failed')


  
  
      
    ) dbt_internal_test