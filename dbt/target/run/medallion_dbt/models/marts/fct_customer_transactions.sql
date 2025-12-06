
  
    
    

    create  table
      "medallion"."main"."fct_customer_transactions__dbt_tmp"
  
    as (
      with base as (
    select * from "medallion"."main"."stg_transactions"
)

-- TODO: Completar el modelo para que cree la tabla fct_customer_transactions con las metricas en schema.yml.

-- queries
select
    customer_id,
    count(*) as transaction_count,
    sum(case when status = 'completed' then amount else 0 end) as total_amount_completed,
    sum(amount) as total_amount_all
from base
group by customer_id
    );
  
  