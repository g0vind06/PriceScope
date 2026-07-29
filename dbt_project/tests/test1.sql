--Fails for scripts that returns rows.
--Passes for scripts that returns no rows.

SELECT * FROM {{ ref('stg_smartprix_prices') }}