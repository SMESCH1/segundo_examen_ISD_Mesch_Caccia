

-- TODO: Implementar el test para verificar que los valores en la columna son no negativos y no nulos.

select
    transaction_count
from "medallion"."main"."fct_customer_transactions"
where transaction_count < 0 or transaction_count is null

