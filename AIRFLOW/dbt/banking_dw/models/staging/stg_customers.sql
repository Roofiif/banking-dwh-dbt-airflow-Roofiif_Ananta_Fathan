-- Staging model for customers
-- Cleans and standardizes raw customer data

{{ config(
    materialized='view',
    tags=['staging', 'customers']
) }}

with source as (
    select * from {{ source('raw', 'customers') }}
),

cleaned as (
    select
        customer_id,
        lower(email) as email,
        first_name,
        last_name,
        first_name || ' ' || last_name AS full_name,
        gender,
        date_of_birth::date as date_of_birth,
        address,
        city,
        province,
        postal_code,
        phone AS phone_number,
        occupation,
        income_level,
        risk_rating,
        registration_date::date as created_at,
        last_updated::date as updated_at
    from source
    where email is not null
)

select * from cleaned
