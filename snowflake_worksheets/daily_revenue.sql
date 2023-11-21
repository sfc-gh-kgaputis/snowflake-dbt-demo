use role sysadmin;
create warehouse if not exists dbt_demo_wh;
use warehouse dbt_demo_wh;
create database if not exists dbt_demo_db;
use schema dbt_demo_db.public;

create warehouse if not exists dbt_transform_wh WAREHOUSE_SIZE=XSMALL INITIALLY_SUSPENDED=TRUE AUTO_RESUME=true AUTO_SUSPEND=60;
create warehouse if not exists dbt_transform_xl_wh WAREHOUSE_SIZE=XLARGE INITIALLY_SUSPENDED=TRUE AUTO_RESUME=true AUTO_SUSPEND=60;


----------------------------------------
-- Raw table DDL
----------------------------------------

use schema raw;

CREATE TABLE company_revenue (
                                 as_of_date DATE,
                                 company_id INTEGER,
                                 plan_id INTEGER,
                                 revenue FLOAT
);
CREATE TABLE plan_details (
                              as_of_date DATE,
                              plan_id INTEGER,
                              price FLOAT
);

----------------------------------------
-- Data generation
----------------------------------------

-- Generating 1000 days of data for plan_details
INSERT INTO plan_details (as_of_date, plan_id, price)
WITH date_sequence AS (
    SELECT
        DATEADD(day, -seq4(), CURRENT_DATE) AS as_of_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1000)) -- Generate 1000 days
),
     plan_ids AS (
         SELECT seq4() % 100000 + 1 AS plan_id
FROM TABLE(GENERATOR(ROWCOUNT => 100000)) -- Generate plans
    )
SELECT
    d.as_of_date,
    p.plan_id,
    uniform(9.99::float, 99.99::float, random())::numeric(5,2) -- Random price between 9.99 and 99.99
FROM date_sequence d
         CROSS JOIN plan_ids p
ORDER BY 1, 2;

-- Generating data for company_revenue
INSERT INTO company_revenue (as_of_date, company_id, plan_id, revenue)
WITH date_sequence AS (
    SELECT DATEADD(day, -seq4(), CURRENT_DATE) AS as_of_date
    FROM TABLE(GENERATOR(ROWCOUNT => 1000)) -- Generate 1000 days
),
     plan_ids AS (
         SELECT (seq8() % 100000 + 1)::integer AS plan_id
         FROM TABLE(GENERATOR(ROWCOUNT => 100000)) -- Generate 100000 plans
     ),
     company_ids AS (
         SELECT (seq4() % 10 + 1)::integer AS company_id
         FROM TABLE(GENERATOR(ROWCOUNT => 10)) -- Generate 10 companies
     ),
     duplicated_results AS (
         SELECT
             d.as_of_date,
             c.company_id,
             p.plan_id,
             uniform(9.99::float, 99.99::float, random())::numeric(5,2) as revenue -- Random revenue between 9.99 and 99.99
         FROM date_sequence d
                  CROSS JOIN plan_ids p
                  CROSS JOIN company_ids c
     ),
     deduplicated AS (
         SELECT
             as_of_date,
             company_id,
             plan_id,
             ANY_VALUE(revenue) AS revenue -- Selecting any one value for revenue
         FROM duplicated_results
         GROUP BY as_of_date, company_id, plan_id
     )
SELECT * FROM deduplicated
ORDER BY 1, 2, 3;

----------------------------------------
-- Manually cluster a table
----------------------------------------

-- This will rewrite a table in place to fix pruning issues
-- Alternatively you can add cluster keys to the table, which will automatically cluster the data
insert overwrite into plan_details
select * from  plan_details
order by as_of_date, plan_id;


----------------------------------------
-- Validate dataset uniqueness
----------------------------------------

SELECT
    as_of_date,
    plan_id,
    COUNT(*) AS count
FROM
    plan_details
GROUP BY
    as_of_date, plan_id
HAVING
    COUNT(*) > 1;


SELECT
    as_of_date,
    company_id,
    plan_id,
    COUNT(*) AS count
FROM
    company_revenue
GROUP BY
    as_of_date,
    company_id,
    plan_id
HAVING
    COUNT(*) > 1;

----------------------------------------
-- Pruning test query
----------------------------------------

WITH revenue_data AS (
    SELECT
        cr.as_of_date,
        cr.company_id,
        cr.plan_id,
        cr.revenue
    FROM company_revenue cr
    WHERE AS_OF_DATE > '2023-11-01'

),
     plan_data AS (
         SELECT
             pd.as_of_date,
             pd.plan_id,
             pd.price
         FROM plan_details pd
         WHERE AS_OF_DATE > '2023-11-01'
     )
SELECT
    rd.as_of_date,
    rd.company_id,
    rd.plan_id,
    rd.revenue,
    pd.price,
    rd.revenue / pd.price AS units_sold
FROM revenue_data rd
         JOIN plan_data pd
              ON rd.plan_id = pd.plan_id
                  AND rd.as_of_date = pd.as_of_date;
