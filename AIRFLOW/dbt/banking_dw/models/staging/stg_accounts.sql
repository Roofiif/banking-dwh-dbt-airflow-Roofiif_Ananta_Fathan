-- Staging model for accounts
-- Cleans and standardizes raw account data

{{ config(
    materialized='view',
    tags=['staging', 'accounts']
) }}

with source as (
    select * from {{ source('raw', 'accounts') }}
),

cleaned as (
    select
        account_id,
        customer_id,
        account_number,
        account_type,
        account_status,
        open_date::date as open_date,
        close_date::date as close_date,
        branch_id,
        branch_name,
        interest_rate::numeric as interest_rate,
        minimum_balance::numeric as minimum_balance,
        current_balance::numeric as current_balance,
        currency,
        last_transaction_date::date as last_transaction_date
    from source
)

select * from cleaned