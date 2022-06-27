select
    order_id,
    sum(amount) as tot_amt
from
    {{ ref ('stg_payment') }}
group by 1
having not(tot_amt >= 0)