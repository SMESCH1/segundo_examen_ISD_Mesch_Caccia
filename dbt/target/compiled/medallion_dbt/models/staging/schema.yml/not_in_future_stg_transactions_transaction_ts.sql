

select transaction_ts
from "medallion"."main"."stg_transactions"
where transaction_ts > current_timestamp

