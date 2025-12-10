
  
    
    

    create  table
      "medallion"."main"."fct_customer_transactions__dbt_tmp"
  
    as (
      with base as (
    select * from "medallion"."main"."stg_transactions"
)

-- queries

select
    customer_id,
    count(*) as transaction_count,
    sum(case when status = 'completed' then amount else 0 end) as total_amount_completed,
    sum(amount) as total_amount_all,
    min(transaction_ts) as first_transaction_ts,   
    max(transaction_ts) as last_transaction_ts     
from "medallion"."main"."stg_transactions"
group by customer_id
    );
  
  