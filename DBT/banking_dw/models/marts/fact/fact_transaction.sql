-- Fact model for banking transactions
-- Grain: one row per transaction_id

{{ config(
    materialized='table',
    tags=['mart', 'fact', 'transaction']
) }}

with tx as (
    select
        transaction_id,
        account_id,
        transaction_date,
        transaction_time,
        transaction_type,
        amount,
        balance_after,
        merchant_name,
        merchant_category,
        channel,
        location,
        status
    from {{ ref('stg_transactions') }}
),

acc as (
    select
        account_id,
        customer_id
    from {{ ref('stg_accounts') }}
),

cust as (
    select
        customer_id,
        customer_sk
    from {{ ref('dim_customers') }}
)

select
    row_number() over (order by tx.transaction_id) as transaction_sk,
    tx.transaction_id as transaction_id_source,

    tx.transaction_date,
    tx.transaction_time,

    acc.account_id,
    cust.customer_sk,

    tx.transaction_type,
    tx.amount,
    tx.balance_after,
    tx.channel,
    tx.status,
    tx.merchant_name,
    tx.merchant_category,
    tx.location
from tx
join acc
    on acc.account_id = tx.account_id
join cust
    on cust.customer_id = acc.customer_id
