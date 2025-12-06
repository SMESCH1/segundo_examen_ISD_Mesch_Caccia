


with source as (
    select *
    from read_parquet(
        '/home/sebastian/Escritorio/MIA/MIA_soft_ing/examen_ing_de_sw_n_data_final/data/clean/transactions_20251201_clean.parquet'
    )
)

-- TODO: Completar el modelo para que cree la tabla staging con los tipos adecuados segun el schema.yml.

select
    cast(transaction_id as integer) as transaction_id,
    cast(customer_id as integer) as customer_id,
    cast(amount as decimal(10, 2)) as amount,
    cast(status as varchar) as status,
    cast(transaction_ts as timestamp) as transaction_ts,
    cast(transaction_date as date) as transaction_date
from source