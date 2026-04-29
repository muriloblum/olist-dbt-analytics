{{ config(materialized='table') }}

with orders as (
    select *
    from {{ ref('stg_olist_orders') }}
)

-- pre-aggregate items to avoid fan-out (n items per order)
, order_items as (
    select
        order_id
      , sum(item_price)        as order_revenue
      , sum(freight_value)     as order_freight
      , count(order_item_id)   as items_count
    from {{ ref('stg_olist_order_items') }}
    group by 1
)

-- pre-aggregate payments to avoid fan-out (n installments per order)
, payments as (
    select
        order_id
      , sum(payment_value)     as total_payment
    from {{ ref('stg_olist_order_payments') }}
    group by 1
)

-- staging guarantees one review per order; max() retrieves that single value
, reviews as (
    select
        order_id
      , max(review_score)      as review_score
    from {{ ref('stg_olist_order_reviews') }}
    group by 1
)

, customers as (
    select
        customer_id
      , city                   as customer_city
      , state                  as customer_state
    from {{ ref('stg_olist_customers') }}
)

select
    -- identifiers
    o.order_id
  , o.customer_id

    -- dates
  , o.order_purchase_at                                                   as purchased_at
  , date_trunc('month', o.order_purchase_at)                              as order_month
  , o.order_delivered_customer_at                                         as delivered_at
  , o.order_estimated_delivery_at                                         as estimated_delivery_at

    -- status
  , o.order_status

    -- delivery metrics (null for undelivered orders — filter by status downstream)
  , case
        when o.order_delivered_customer_at is not null
        then date_diff('day', o.order_purchase_at, o.order_delivered_customer_at)
    end                                                                   as delivery_days

  , case
        when o.order_delivered_customer_at is not null
         and o.order_estimated_delivery_at is not null
        then greatest(0, date_diff('day', o.order_estimated_delivery_at, o.order_delivered_customer_at))
    end                                                                   as delay_days

    -- null for undelivered orders; false would misclassify in-transit orders as late
  , case
        when o.order_delivered_customer_at is null
          or o.order_estimated_delivery_at is null    then null
        when o.order_delivered_customer_at <= o.order_estimated_delivery_at then true
        else false
    end                                                                   as is_on_time

    -- financial metrics
  , coalesce(oi.order_revenue, 0)                                         as order_revenue
  , coalesce(oi.order_freight, 0)                                         as order_freight
  , coalesce(p.total_payment, 0)                                          as total_payment
  , coalesce(oi.items_count, 0)                                           as items_count

    -- excludes canceled and unavailable orders from revenue reporting
  , case
        when o.order_status in ('canceled', 'unavailable') then 0
        else coalesce(p.total_payment, 0)
    end                                                                   as recognized_revenue

    -- customer geography
  , c.customer_city
  , c.customer_state

    -- cx
  , r.review_score

from orders as o
left join order_items   as oi on o.order_id    = oi.order_id
left join payments      as p  on o.order_id    = p.order_id
left join customers     as c  on o.customer_id = c.customer_id
left join reviews       as r  on o.order_id    = r.order_id
