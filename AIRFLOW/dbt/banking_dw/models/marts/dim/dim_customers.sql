-- Dimension model for customers

{{ config(
    materialized='table',
    tags=['mart', 'dimension', 'customers']
) }}

with base as (
    select
        customer_id,
        email,
        full_name,
        gender,
        date_of_birth,
        address,
        city,
        province,
        postal_code,
        phone_number,
        occupation,
        income_level,
        risk_rating
    from {{ ref('stg_customers') }}
)

select
    row_number() over (order by customer_id) as customer_sk,
    customer_id,
    email,
    full_name,
    gender,
    date_of_birth,
    address,
    city,
    province,
    postal_code,
    phone_number,
    occupation,
    income_level,
    risk_rating
from base
