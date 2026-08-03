--Fails for scripts that returns rows.
--Passes for scripts that returns no rows.

SELECT * FROM "dev"."marts_staging"."stg_smartprix_prices" WHERE mrp < 0  --negative mrp is invalid, so this should return no rows. If it returns rows, then the test fails.