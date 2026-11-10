-- Kunlik daromad: bekor qilingan buyurtmalar hisobga olinmaydi
select
    order_date,
    count(*)                       as orders_count,
    sum(amount)                    as total_revenue,
    round(avg(amount), 2)          as avg_order_value
from {{ source('staging', 'orders') }}
where status <> 'cancelled'
group by order_date
