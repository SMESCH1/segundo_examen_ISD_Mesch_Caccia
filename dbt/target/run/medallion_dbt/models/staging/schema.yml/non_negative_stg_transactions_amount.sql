
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

-- TODO: Implementar el test para verificar que los valores en la columna son no negativos y no nulos.

select
    amount
from "medallion"."main"."stg_transactions"
where amount < 0 or amount is null


  
  
      
    ) dbt_internal_test