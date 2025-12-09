
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

with silver as (
    select distinct customer_id
    from "medallion"."main"."stg_transactions"
)
select g.customer_id
from "medallion"."main"."fct_customer_transactions" g
left join silver s
    on g.customer_id = s.customer_id
where s.customer_id is null


  
  
      
    ) dbt_internal_test