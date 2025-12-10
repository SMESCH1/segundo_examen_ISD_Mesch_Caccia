
with base as (
    select * from {{ ref('stg_transactions') }}
)

-- TODO: Completar el modelo para que cree la tabla fct_customer_transactions con las metricas en schema.yml.

-- queries

select
    customer_id,
    count(*) as transaction_count,
    sum(case when status = 'completed' then amount else 0 end) as total_amount_completed,
    sum(amount) as total_amount_all,
    min(transaction_ts) as first_transaction_ts,   
    max(transaction_ts) as last_transaction_ts     
from {{ ref('stg_transactions') }}
group by customer_id
