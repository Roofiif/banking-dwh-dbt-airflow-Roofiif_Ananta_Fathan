-- Intermediate model
-- Calculates end of day balance per account and date

{{ config(
    materialized='table',
    tags=['intermediate', 'daily_balance']
) }}

with ordered_tx as (
    select
        transaction_id,
        account_id,
        transaction_date,
        transaction_time,
        balance_after,
        row_number() over (
            partition by account_id, transaction_date
            order by transaction_time desc, transaction_id desc
        ) as rn
    from {{ ref('stg_transactions') }}
)

select
    account_id,
    transaction_date as balance_date,
    balance_after as end_of_day_balance
from ordered_tx
where rn = 1
