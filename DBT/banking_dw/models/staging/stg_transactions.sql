-- Staging model for transactions
-- Cleans and standardizes raw transaction data

{{ config(
    materialized='view',
    tags=['staging', 'transactions']
) }}

with source as (
    select * from {{ source('raw', 'transactions') }}
),

cleaned as (
    select
        transaction_id,
        account_id,
        transaction_date::date as transaction_date,
        transaction_time::time as transaction_time,
        transaction_type,
        amount::numeric as amount,
        balance_after::numeric as balance_after,
        merchant_name,
        merchant_category,
        channel,
        location,
        description,
        status
    from source
)

select * from cleaned
