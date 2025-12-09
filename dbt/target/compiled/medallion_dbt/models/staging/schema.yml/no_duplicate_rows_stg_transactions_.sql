

select *
from "medallion"."main"."stg_transactions"
group by *
having count(*) > 1

