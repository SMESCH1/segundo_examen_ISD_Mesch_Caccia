
  
  create view "medallion"."main"."stg_transactions__dbt_tmp" as (
    


with source as (
    select *
    from read_parquet(
        '/home/matias/Documentos/UdeSA/MIA/T3/ISD_examenes/segundo_examen_ISD_Mesch_Caccia/data/clean/transactions_20251201_clean.parquet'
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
  );
