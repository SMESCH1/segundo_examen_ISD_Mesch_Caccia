
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

-- TODO: Implementar el test para verificar que los valores en la columna son no negativos y no nulos.

select
    total_amount_completed
from "medallion"."main"."fct_customer_transactions"
where total_amount_completed < 0 or total_amount_completed is null


  
  
      
    ) dbt_internal_test