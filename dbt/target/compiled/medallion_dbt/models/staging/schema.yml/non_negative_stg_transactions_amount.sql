

-- TODO: Implementar el test para verificar que los valores en la columna son no negativos y no nulos.

select
    amount
from "medallion"."main"."stg_transactions"
where amount < 0 or amount is null

