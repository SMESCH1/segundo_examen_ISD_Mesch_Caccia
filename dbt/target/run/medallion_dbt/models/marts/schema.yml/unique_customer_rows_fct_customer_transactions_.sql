
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

select customer_id
from "medallion"."main"."fct_customer_transactions"
group by customer_id
having count(*) > 1


  
  
      
    ) dbt_internal_test