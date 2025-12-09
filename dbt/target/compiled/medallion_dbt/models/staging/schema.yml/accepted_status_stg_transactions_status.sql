

select status
from "medallion"."main"."stg_transactions"
where status not in ('completed', 'pending', 'failed')

