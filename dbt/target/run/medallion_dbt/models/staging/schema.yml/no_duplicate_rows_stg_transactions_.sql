
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select *
from "medallion"."main"."stg_transactions"
group by *
having count(*) > 1


  
  
      
    ) dbt_internal_test