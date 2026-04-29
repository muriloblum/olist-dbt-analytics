{{ config(materialized='table') }}

with sellers as (
    select
        seller_id
      , city    as seller_city
      , state   as seller_state
    from {{ ref('stg_olist_sellers') }}
)

-- one row per (seller, order) — resolves fan-out before final aggregation
-- order-level metrics pulled from mart_orders_wide to avoid recomputing logic
, seller_orders as (
    select
        oi.seller_id
      , oi.order_id
      , sum(oi.item_price)              as order_revenue
      , count(oi.order_item_id)         as items_count
      , max(o.delivery_days)            as delivery_days
      , max(o.delay_days)               as delay_days
      , max(cast(o.is_on_time as int))  as is_on_time
      , max(o.review_score)             as review_score
    from {{ ref('stg_olist_order_items') }} as oi
    inner join {{ ref('mart_orders_wide') }} as o
        on oi.order_id = o.order_id
    where o.order_status = 'delivered'
    group by 1, 2
)

select
    -- seller info
    s.seller_id
  , s.seller_city
  , s.seller_state

    -- volume
  , count(so.order_id)                                                    as total_orders
  , coalesce(sum(so.items_count), 0)                                      as total_items_sold

    -- financials
  , coalesce(sum(so.order_revenue), 0)                                    as total_revenue

    -- weighted average price: total revenue divided by total items sold
  , sum(so.order_revenue) / nullif(sum(so.items_count), 0)                as avg_revenue_per_item

    -- delivery metrics
  , round(avg(so.delivery_days), 1)                                       as avg_delivery_days
  , round(avg(so.delay_days), 1)                                          as avg_delay_days
  , round(
        100.0 * sum(so.is_on_time)
        / nullif(count(so.order_id), 0)
    , 2)                                                                  as on_time_pct

    -- cx metrics
  , round(avg(so.review_score), 2)                                        as avg_review_score
  , count(so.review_score)                                                as total_reviews

from sellers as s
left join seller_orders as so
    on s.seller_id = so.seller_id
group by 1, 2, 3
