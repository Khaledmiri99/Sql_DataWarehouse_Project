-- Calculate the total sales per year
-- and the total customers per year 
SELECT 
EXTRACT(YEAR FROM order_date ) AS order_date_year, --Year granularity
SUM(sales_amount) AS total_sales_amount_per_year,
COUNT(customer_key) AS total_customers
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date_year
ORDER BY order_date_year

-- Calculate the total sales per month 
-- and the running total of sales over time 
SELECT 
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
AVG(avg_price) OVER (ORDER BY order_date) AS running_average_price
FROM (
SELECT
DATE_TRUNC('year', order_date)::date AS order_date,
SUM (sales_amount) AS total_sales,
AVG(sls_price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('year', order_date)::date
ORDER BY order_date
)