
  
  create view "medallion"."main"."stg_transactions__dbt_tmp" as (
    


with source as (
    select *
    from read_parquet(
        '/home/sebastian/Escritorio/MIA/MIA_soft_ing/examen_ing_de_sw_n_data_final/data/clean/transactions_20251201_clean.parquet'
    )
)

-- TODO: Completar el modelo para que cree la tabla staging con los tipos adecuados segun el schema.yml.
  );
