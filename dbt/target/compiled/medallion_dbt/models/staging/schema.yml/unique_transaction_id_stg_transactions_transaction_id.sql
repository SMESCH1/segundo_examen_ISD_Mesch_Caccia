

select transaction_id
from "medallion"."main"."stg_transactions"
group by transaction_id
having count(*) > 1

