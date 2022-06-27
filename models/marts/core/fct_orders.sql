with orders as (
    select 
        * 
    from 
        {{ ref ('stg_orders') }}
),

payments as (
    select 
        * 
    from 
        {{ ref ('stg_payment') }}
),

order_payments as (
    select
        order_id,
        sum(amount) as amount
    from    
        payments
    where status = 'success'
    group by order_id
)

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        order_payments.amount
    from
        orders
    left join
        order_payments
    on orders.order_id = order_payments.order_id
